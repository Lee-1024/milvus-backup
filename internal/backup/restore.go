package backup

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func Restore(ctx context.Context, client *milvusclient.Client, opts RestoreOptions) error {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	dbName := displayDatabase(opts.Database)
	fmt.Printf("restore started: database=%s input=%s batch_size=%d progress_every=%d drop_existing=%v name_suffix=%q\n", dbName, opts.InputDir, opts.BatchSize, opts.ProgressEvery, opts.DropExisting, opts.NameSuffix)

	var manifest Manifest
	if err := readJSON(filepath.Join(opts.InputDir, manifestFile), &manifest); err != nil {
		return err
	}
	fmt.Printf("manifest loaded: database=%s input=%s collections=%d created_at=%s\n", dbName, opts.InputDir, len(manifest.Collections), manifest.CreatedAt.Format(time.RFC3339))

	selected := make(map[string]bool, len(opts.Collections))
	for _, name := range opts.Collections {
		selected[name] = true
	}

	for _, coll := range manifest.Collections {
		if len(selected) > 0 && !selected[coll.Name] {
			continue
		}
		targetName := coll.Name + opts.NameSuffix
		fmt.Printf("collection restore started: database=%s source_collection=%s target_collection=%s expected_rows=%d\n", dbName, coll.Name, targetName, coll.RowCount)
		if err := restoreCollection(ctx, client, opts, coll, targetName); err != nil {
			return err
		}
		fmt.Printf("collection restore finished: database=%s source_collection=%s target_collection=%s expected_rows=%d\n", dbName, coll.Name, targetName, coll.RowCount)
	}
	return nil
}

func restoreCollection(ctx context.Context, client *milvusclient.Client, opts RestoreOptions, coll CollectionManifest, targetName string) error {
	exists, err := client.HasCollection(ctx, milvusclient.NewHasCollectionOption(targetName))
	if err != nil {
		return fmt.Errorf("check collection %s: %w", targetName, err)
	}
	if exists {
		if !opts.DropExisting {
			return fmt.Errorf("collection %s already exists; pass -drop-existing or -name-suffix", targetName)
		}
		fmt.Printf("dropping existing collection: database=%s collection=%s\n", displayDatabase(opts.Database), targetName)
		if err := client.DropCollection(ctx, milvusclient.NewDropCollectionOption(targetName)); err != nil {
			return fmt.Errorf("drop collection %s: %w", targetName, err)
		}
	}

	schema := cloneSchema(coll.Schema, targetName)
	fmt.Printf("creating collection: database=%s collection=%s fields=%d shard_num=%d consistency=%v\n", displayDatabase(opts.Database), targetName, len(schema.Fields), coll.ShardNum, coll.ConsistencyLevel)
	create := milvusclient.NewCreateCollectionOption(targetName, schema)
	if coll.ShardNum > 0 {
		create = create.WithShardNum(coll.ShardNum)
	}
	if coll.ConsistencyLevel != 0 {
		create = create.WithConsistencyLevel(coll.ConsistencyLevel)
	}
	if err := client.CreateCollection(ctx, create); err != nil {
		return fmt.Errorf("create collection %s: %w", targetName, err)
	}

	for _, partition := range coll.Partitions {
		if partition == "_default" {
			continue
		}
		fmt.Printf("creating partition: database=%s collection=%s partition=%s\n", displayDatabase(opts.Database), targetName, partition)
		if err := client.CreatePartition(ctx, milvusclient.NewCreatePartitionOption(targetName, partition)); err != nil {
			return fmt.Errorf("create partition %s/%s: %w", targetName, partition, err)
		}
	}

	if err := importRows(ctx, client, filepath.Join(opts.InputDir, coll.DataFile), schema, targetName, opts.BatchSize, opts.ProgressEvery); err != nil {
		return err
	}
	fmt.Printf("flushing collection: database=%s collection=%s\n", displayDatabase(opts.Database), targetName)
	if _, err := client.Flush(ctx, milvusclient.NewFlushOption(targetName)); err != nil {
		return fmt.Errorf("flush %s: %w", targetName, err)
	}
	return nil
}

func importRows(ctx context.Context, client *milvusclient.Client, path string, schema *entity.Schema, collectionName string, batchSize int, progressEvery int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var rows []Row
	var total int64
	var batch int64
	nextProgress := progressEvery
	started := time.Now()
	reader := bufio.NewReaderSize(f, 1024*1024)
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		batch++
		pending := len(rows)
		fmt.Printf("insert batch started: collection=%s batch=%d rows=%d total_before=%d elapsed=%s\n", collectionName, batch, pending, total, time.Since(started).Round(time.Second))
		cols, err := columnsFromRows(schema, rows)
		if err != nil {
			return err
		}
		_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName, cols...))
		if err != nil {
			return err
		}
		total += int64(pending)
		fmt.Printf("insert batch finished: collection=%s batch=%d total_rows=%d elapsed=%s\n", collectionName, batch, total, time.Since(started).Round(time.Second))
		if progressEvery > 0 && total >= nextProgress {
			fmt.Printf("restore progress: collection=%s rows=%d elapsed=%s\n", collectionName, total, time.Since(started).Round(time.Second))
			nextProgress += progressEvery
		}
		rows = rows[:0]
		return nil
	}

	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			var row Row
			if decErr := json.Unmarshal(line, &row); decErr != nil {
				return decErr
			}
			rows = append(rows, row)
			if len(rows) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if err := flush(); err != nil {
		return err
	}
	fmt.Printf("import rows finished: collection=%s rows=%d batches=%d elapsed=%s\n", collectionName, total, batch, time.Since(started).Round(time.Second))
	return nil
}

func readJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(v)
}
