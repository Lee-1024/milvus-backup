package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lijinghua/milvus-backup/internal/backup"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch os.Args[1] {
	case "backup":
		if err := runBackup(ctx, os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "restore":
		if err := runRestore(ctx, os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func runBackup(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	cfg := commonFlags(fs)
	out := fs.String("out", "backup", "backup output directory")
	collections := fs.String("collections", "", "comma-separated collection names; empty means all collections")
	batchSize := fs.Int("batch-size", 1000, "query iterator batch size")
	progressEvery := fs.Int64("progress-every", 10000, "print progress every N rows; 0 disables row progress logs")
	filter := fs.String("filter", "", "optional Milvus filter expression applied to every collection")
	timeout := fs.Duration("timeout", 0, "operation timeout, for example 30m; 0 disables timeout")
	_ = fs.Parse(args)

	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	client, err := backup.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close(ctx)

	opts := backup.BackupOptions{
		OutputDir:     *out,
		Collections:   splitCSV(*collections),
		BatchSize:     *batchSize,
		ProgressEvery: *progressEvery,
		Filter:        *filter,
		Database:      cfg.DBName,
		StartedAtUTC:  time.Now().UTC(),
	}
	return backup.Backup(ctx, client, opts)
}

func runRestore(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	cfg := commonFlags(fs)
	in := fs.String("in", "backup", "backup input directory")
	collections := fs.String("collections", "", "comma-separated collection names; empty means all collections from manifest")
	batchSize := fs.Int("batch-size", 1000, "insert batch size")
	progressEvery := fs.Int64("progress-every", 10000, "print progress every N rows; 0 disables row progress logs")
	dropExisting := fs.Bool("drop-existing", false, "drop existing collections before restore")
	suffix := fs.String("name-suffix", "", "append suffix to restored collection names")
	timeout := fs.Duration("timeout", 0, "operation timeout, for example 30m; 0 disables timeout")
	_ = fs.Parse(args)

	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	client, err := backup.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close(ctx)

	opts := backup.RestoreOptions{
		InputDir:      *in,
		Collections:   splitCSV(*collections),
		BatchSize:     *batchSize,
		ProgressEvery: *progressEvery,
		DropExisting:  *dropExisting,
		NameSuffix:    *suffix,
		Database:      cfg.DBName,
		StartedAtUTC:  time.Now().UTC(),
	}
	return backup.Restore(ctx, client, opts)
}

func commonFlags(fs *flag.FlagSet) backup.ClientConfig {
	cfg := backup.ClientConfig{}
	fs.StringVar(&cfg.Address, "address", envOr("MILVUS_ADDRESS", "localhost:19530"), "Milvus address")
	fs.StringVar(&cfg.Username, "username", os.Getenv("MILVUS_USERNAME"), "Milvus username")
	fs.StringVar(&cfg.Password, "password", os.Getenv("MILVUS_PASSWORD"), "Milvus password")
	fs.StringVar(&cfg.APIKey, "api-key", os.Getenv("MILVUS_API_KEY"), "Milvus API key")
	fs.StringVar(&cfg.DBName, "db", os.Getenv("MILVUS_DB"), "Milvus database name")
	fs.BoolVar(&cfg.TLS, "tls", false, "enable TLS")
	return cfg
}

func splitCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func usage() {
	fmt.Fprintln(os.Stderr, `milvus-backup is a generic Milvus collection backup/restore tool.

Usage:
  milvus-backup backup  -address localhost:19530 -out ./backup
  milvus-backup restore -address localhost:19530 -in ./backup -drop-existing

Environment:
  MILVUS_ADDRESS, MILVUS_USERNAME, MILVUS_PASSWORD, MILVUS_API_KEY, MILVUS_DB`)
}
