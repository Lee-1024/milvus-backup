package backup

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func Backup(ctx context.Context, client *milvusclient.Client, opts BackupOptions) error {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return err
	}
	dbName := displayDatabase(opts.Database)
	fmt.Printf("backup started: database=%s output=%s batch_size=%d progress_every=%d\n", dbName, opts.OutputDir, opts.BatchSize, opts.ProgressEvery)

	names := opts.Collections
	if len(names) == 0 {
		fmt.Printf("listing collections: database=%s\n", dbName)
		var err error
		names, err = client.ListCollections(ctx, milvusclient.NewListCollectionOption())
		if err != nil {
			return fmt.Errorf("list collections: %w", err)
		}
	}
	fmt.Printf("collections selected: database=%s count=%d names=%v\n", dbName, len(names), names)

	manifest := Manifest{Version: 1, CreatedAt: opts.StartedAtUTC}
	allStarted := time.Now()
	for i, name := range names {
		collectionStarted := time.Now()
		fmt.Printf("collection backup started: database=%s collection=%s index=%d/%d\n", dbName, name, i+1, len(names))
		if err := ensureCollectionExists(ctx, client, dbName, name); err != nil {
			if skipCollection(&manifest, opts, name, err) {
				continue
			}
			return err
		}
		coll, err := client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(name))
		if err != nil {
			err = fmt.Errorf("describe collection %s: %w", name, err)
			if skipCollection(&manifest, opts, name, err) {
				continue
			}
			return err
		}
		fmt.Printf("collection schema loaded: database=%s collection=%s fields=%d shard_num=%d consistency=%v\n", dbName, name, len(coll.Schema.Fields), coll.ShardNum, coll.ConsistencyLevel)
		partitions, err := client.ListPartitions(ctx, milvusclient.NewListPartitionOption(name))
		if err != nil {
			err = fmt.Errorf("list partitions %s: %w", name, err)
			if skipCollection(&manifest, opts, name, err) {
				continue
			}
			return err
		}
		fmt.Printf("collection partitions loaded: database=%s collection=%s partitions=%v\n", dbName, name, partitions)

		dataFile := fmt.Sprintf("%s.jsonl", name)
		rows, err := exportCollection(ctx, client, name, filepath.Join(opts.OutputDir, dataFile), opts)
		if err != nil {
			if skipCollection(&manifest, opts, name, err) {
				continue
			}
			return err
		}
		manifest.Collections = append(manifest.Collections, CollectionManifest{
			Name:             name,
			Schema:           coll.Schema,
			Partitions:       partitions,
			ConsistencyLevel: coll.ConsistencyLevel,
			ShardNum:         coll.ShardNum,
			Properties:       coll.Properties,
			RowCount:         rows,
			DataFile:         dataFile,
		})
		fmt.Printf("collection backup finished: database=%s collection=%s rows=%d elapsed=%s total_elapsed=%s\n", dbName, name, rows, time.Since(collectionStarted).Round(time.Second), time.Since(allStarted).Round(time.Second))
	}

	fmt.Printf("writing manifest: database=%s file=%s collections=%d skipped=%d\n", dbName, filepath.Join(opts.OutputDir, manifestFile), len(manifest.Collections), len(manifest.SkippedCollections))
	return writeJSON(filepath.Join(opts.OutputDir, manifestFile), manifest)
}

func skipCollection(manifest *Manifest, opts BackupOptions, name string, err error) bool {
	if !opts.SkipFailed {
		return false
	}
	manifest.SkippedCollections = append(manifest.SkippedCollections, SkippedCollection{
		Name:  name,
		Error: err.Error(),
	})
	fmt.Printf("collection backup skipped: database=%s collection=%s error=%q\n", displayDatabase(opts.Database), name, err.Error())
	return true
}

func ensureCollectionExists(ctx context.Context, client *milvusclient.Client, dbName, name string) error {
	exists, err := client.HasCollection(ctx, milvusclient.NewHasCollectionOption(name))
	if err != nil {
		return fmt.Errorf("check collection %s in database %s: %w", name, dbName, err)
	}
	if exists {
		return nil
	}
	available, listErr := client.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if listErr != nil {
		return fmt.Errorf("collection %s not found in database %s; also failed to list collections: %w", name, dbName, listErr)
	}
	return fmt.Errorf("collection %s not found in database %s; available collections: %v", name, dbName, available)
}

func exportCollection(ctx context.Context, client *milvusclient.Client, name, file string, opts BackupOptions) (int64, error) {
	f, err := os.Create(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	writer := bufio.NewWriterSize(f, 1024*1024)
	defer writer.Flush()

	started := time.Now()
	dbName := displayDatabase(opts.Database)
	batchSize := opts.BatchSize
	fmt.Printf("query iterator opening: database=%s collection=%s file=%s filter=%q batch_size=%d\n", dbName, name, file, opts.Filter, batchSize)
	iter, err := openQueryIteratorWithRetry(ctx, client, name, opts, batchSize)
	if err != nil {
		return 0, fmt.Errorf("create query iterator %s: %w", name, err)
	}
	fmt.Printf("query iterator opened: database=%s collection=%s\n", dbName, name)

	var total int64
	var batch int64
	nextProgress := opts.ProgressEvery
	for {
		rs, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return total, fmt.Errorf("query %s: %w", name, err)
		}
		batch++
		fmt.Printf("query batch received: database=%s collection=%s batch=%d rows=%d total_before=%d elapsed=%s\n", dbName, name, batch, rs.ResultCount, total, time.Since(started).Round(time.Second))
		for i := 0; i < rs.ResultCount; i++ {
			row, err := rowFromColumns(rs.Fields, i)
			if err != nil {
				return total, err
			}
			b, err := json.Marshal(row)
			if err != nil {
				return total, err
			}
			if _, err := writer.Write(append(b, '\n')); err != nil {
				return total, err
			}
			total++
			if opts.ProgressEvery > 0 && total >= nextProgress {
				fmt.Printf("backup progress: database=%s collection=%s rows=%d elapsed=%s\n", dbName, name, total, time.Since(started).Round(time.Second))
				nextProgress += opts.ProgressEvery
			}
		}
	}
	fmt.Printf("query iterator finished: database=%s collection=%s rows=%d batches=%d elapsed=%s\n", dbName, name, total, batch, time.Since(started).Round(time.Second))
	return total, nil
}

func openQueryIteratorWithRetry(ctx context.Context, client *milvusclient.Client, name string, opts BackupOptions, batchSize int) (milvusclient.QueryIterator, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	for {
		iterOpt := milvusclient.NewQueryIteratorOption(name).
			WithBatchSize(batchSize).
			WithOutputFields("*")
		if opts.Filter != "" {
			iterOpt = iterOpt.WithFilter(opts.Filter)
		}

		iter, err := client.QueryIterator(ctx, iterOpt)
		if err == nil {
			if batchSize != opts.BatchSize {
				fmt.Printf("query iterator batch size reduced: database=%s collection=%s original_batch_size=%d actual_batch_size=%d\n", displayDatabase(opts.Database), name, opts.BatchSize, batchSize)
			}
			return iter, nil
		}
		if batchSize <= 1 || !isQueryResultLimitError(err) {
			return nil, err
		}
		nextBatchSize := batchSize / 2
		if nextBatchSize < 1 {
			nextBatchSize = 1
		}
		fmt.Printf("query iterator result too large, retrying with smaller batch: database=%s collection=%s batch_size=%d next_batch_size=%d error=%q\n", displayDatabase(opts.Database), name, batchSize, nextBatchSize, err.Error())
		batchSize = nextBatchSize
	}
}

func isQueryResultLimitError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "query results exceed the limit size") ||
		strings.Contains(msg, "exceed the limit size")
}

func displayDatabase(name string) string {
	if name == "" {
		return "default"
	}
	return name
}

func rowFromColumns(cols []column.Column, idx int) (Row, error) {
	row := Row{}
	for _, col := range cols {
		v, err := col.Get(idx)
		if err != nil {
			return nil, fmt.Errorf("get column %s row %d: %w", col.Name(), idx, err)
		}
		row[col.Name()] = normalizeValue(v)
	}
	return row, nil
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
