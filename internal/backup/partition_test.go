package backup

import "testing"

func TestPartitionDataFileNamesAreStableAndFilesystemSafe(t *testing.T) {
	tests := []struct {
		collection string
		partition  string
		want       string
	}{
		{collection: "docs", partition: "_default", want: "docs.jsonl"},
		{collection: "docs", partition: "p1", want: "docs__partition__p1.jsonl"},
		{collection: "docs/a", partition: "p:b", want: "docs_a__partition__p_b.jsonl"},
	}

	for _, tt := range tests {
		got := partitionDataFile(tt.collection, tt.partition)
		if got != tt.want {
			t.Fatalf("partitionDataFile(%q, %q) = %q, want %q", tt.collection, tt.partition, got, tt.want)
		}
	}
}

func TestCollectionManifestDataFilesReturnsLegacyFileWhenPartitionFilesAreMissing(t *testing.T) {
	coll := CollectionManifest{
		Name:     "docs",
		DataFile: "docs.jsonl",
	}

	files := coll.DataFiles()
	if len(files) != 1 {
		t.Fatalf("len(files) = %d, want 1", len(files))
	}
	if files[0].Partition != "" || files[0].File != "docs.jsonl" {
		t.Fatalf("files[0] = %#v, want legacy docs.jsonl without partition", files[0])
	}
}
