package backup

import (
	"strings"
	"unicode"
)

const defaultPartitionName = "_default"

func partitionDataFile(collection, partition string) string {
	collection = safeFilePart(collection)
	if partition == "" || partition == defaultPartitionName {
		return collection + ".jsonl"
	}
	return collection + "__partition__" + safeFilePart(partition) + ".jsonl"
}

func safeFilePart(v string) string {
	var b strings.Builder
	b.Grow(len(v))
	lastUnderscore := false
	for _, r := range v {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "._")
	if out == "" {
		return "unnamed"
	}
	return out
}
