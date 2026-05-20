package backup

import (
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
)

const manifestFile = "manifest.json"

type ClientConfig struct {
	Address  string
	Username string
	Password string
	APIKey   string
	DBName   string
	TLS      bool
}

type BackupOptions struct {
	OutputDir     string
	Collections   []string
	BatchSize     int
	ProgressEvery int64
	Filter        string
	SkipFailed    bool
	Database      string
	StartedAtUTC  time.Time
}

type RestoreOptions struct {
	InputDir      string
	Collections   []string
	BatchSize     int
	ProgressEvery int64
	DropExisting  bool
	NameSuffix    string
	Database      string
	StartedAtUTC  time.Time
}

type Manifest struct {
	Version            int                  `json:"version"`
	CreatedAt          time.Time            `json:"created_at"`
	Collections        []CollectionManifest `json:"collections"`
	SkippedCollections []SkippedCollection  `json:"skipped_collections,omitempty"`
}

type CollectionManifest struct {
	Name             string                  `json:"name"`
	Schema           *entity.Schema          `json:"schema"`
	Partitions       []string                `json:"partitions"`
	PartitionFiles   []PartitionDataFile     `json:"partition_files,omitempty"`
	ConsistencyLevel entity.ConsistencyLevel `json:"consistency_level"`
	ShardNum         int32                   `json:"shard_num"`
	Properties       map[string]string       `json:"properties,omitempty"`
	RowCount         int64                   `json:"row_count"`
	DataFile         string                  `json:"data_file"`
}

type PartitionDataFile struct {
	Partition string `json:"partition"`
	File      string `json:"file"`
	RowCount  int64  `json:"row_count"`
}

func (m CollectionManifest) DataFiles() []PartitionDataFile {
	if len(m.PartitionFiles) > 0 {
		return m.PartitionFiles
	}
	if m.DataFile == "" {
		return nil
	}
	return []PartitionDataFile{{File: m.DataFile, RowCount: m.RowCount}}
}

type SkippedCollection struct {
	Name  string `json:"name"`
	Error string `json:"error"`
}

type Row map[string]any
