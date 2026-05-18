package backup

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func Restore(ctx context.Context, client *milvusclient.Client, opts RestoreOptions) error {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}

	var manifest Manifest
	if err := readJSON(filepath.Join(opts.InputDir, manifestFile), &manifest); err != nil {
		return err
	}

	selected := make(map[string]bool, len(opts.Collections))
	for _, name := range opts.Collections {
		selected[name] = true
	}

	for _, coll := range manifest.Collections {
		if len(selected) > 0 && !selected[coll.Name] {
			continue
		}
		targetName := coll.Name + opts.NameSuffix
		if err := restoreCollection(ctx, client, opts, coll, targetName); err != nil {
			return err
		}
		fmt.Printf("restored %s as %s: %d rows\n", coll.Name, targetName, coll.RowCount)
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
		if err := client.DropCollection(ctx, milvusclient.NewDropCollectionOption(targetName)); err != nil {
			return fmt.Errorf("drop collection %s: %w", targetName, err)
		}
	}

	schema := cloneSchema(coll.Schema, targetName)
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
		if err := client.CreatePartition(ctx, milvusclient.NewCreatePartitionOption(targetName, partition)); err != nil {
			return fmt.Errorf("create partition %s/%s: %w", targetName, partition, err)
		}
	}

	if err := importRows(ctx, client, filepath.Join(opts.InputDir, coll.DataFile), schema, targetName, opts.BatchSize); err != nil {
		return err
	}
	if _, err := client.Flush(ctx, milvusclient.NewFlushOption(targetName)); err != nil {
		return fmt.Errorf("flush %s: %w", targetName, err)
	}
	return nil
}

func importRows(ctx context.Context, client *milvusclient.Client, path string, schema *entity.Schema, collectionName string, batchSize int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var rows []Row
	reader := bufio.NewReaderSize(f, 1024*1024)
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		cols, err := columnsFromRows(schema, rows)
		if err != nil {
			return err
		}
		_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName, cols...))
		rows = rows[:0]
		return err
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
	return flush()
}

func readJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(v)
}
