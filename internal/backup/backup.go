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

	names := opts.Collections
	if len(names) == 0 {
		var err error
		names, err = client.ListCollections(ctx, milvusclient.NewListCollectionOption())
		if err != nil {
			return fmt.Errorf("list collections: %w", err)
		}
	}

	manifest := Manifest{Version: 1, CreatedAt: opts.StartedAtUTC}
	for _, name := range names {
		coll, err := client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(name))
		if err != nil {
			return fmt.Errorf("describe collection %s: %w", name, err)
		}
		partitions, err := client.ListPartitions(ctx, milvusclient.NewListPartitionOption(name))
		if err != nil {
			return fmt.Errorf("list partitions %s: %w", name, err)
		}

		dataFile := fmt.Sprintf("%s.jsonl", name)
		rows, err := exportCollection(ctx, client, name, filepath.Join(opts.OutputDir, dataFile), opts)
		if err != nil {
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
		fmt.Printf("backed up %s: %d rows\n", name, rows)
	}

	return writeJSON(filepath.Join(opts.OutputDir, manifestFile), manifest)
}

func exportCollection(ctx context.Context, client *milvusclient.Client, name, file string, opts BackupOptions) (int64, error) {
	f, err := os.Create(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	writer := bufio.NewWriterSize(f, 1024*1024)
	defer writer.Flush()

	iterOpt := milvusclient.NewQueryIteratorOption(name).
		WithBatchSize(opts.BatchSize).
		WithOutputFields("*")
	if opts.Filter != "" {
		iterOpt = iterOpt.WithFilter(opts.Filter)
	}

	iter, err := client.QueryIterator(ctx, iterOpt)
	if err != nil {
		return 0, fmt.Errorf("create query iterator %s: %w", name, err)
	}

	var total int64
	for {
		rs, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return total, fmt.Errorf("query %s: %w", name, err)
		}
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
		}
	}
	return total, nil
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
