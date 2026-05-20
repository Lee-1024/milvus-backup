package backup

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/client/v2/entity"
)

func TestDecodeRowPreservesLargeInt64AsJSONNumber(t *testing.T) {
	row, err := decodeRow([]byte(`{"id":9223372036854775807}`))
	if err != nil {
		t.Fatalf("decode row: %v", err)
	}

	n, ok := row["id"].(json.Number)
	if !ok {
		t.Fatalf("id type = %T, want json.Number", row["id"])
	}

	got, err := toInt64(n)
	if err != nil {
		t.Fatalf("to int64: %v", err)
	}
	if got != math.MaxInt64 {
		t.Fatalf("id = %d, want %d", got, int64(math.MaxInt64))
	}
}

func TestToInt8RejectsOutOfRangeValue(t *testing.T) {
	field := &entity.Field{Name: "tiny", DataType: entity.FieldTypeInt8}

	_, err := columnFromRows(field, []Row{{"tiny": json.Number("128")}})
	if err == nil {
		t.Fatal("expected out-of-range error")
	}
	if !strings.Contains(err.Error(), "out of range for int8") {
		t.Fatalf("error = %q, want int8 range context", err.Error())
	}
}

func TestVectorDimRejectsInvalidSchemaDimension(t *testing.T) {
	field := &entity.Field{
		Name:       "embedding",
		DataType:   entity.FieldTypeFloatVector,
		TypeParams: map[string]string{"dim": "not-a-number"},
	}

	_, err := columnFromRows(field, []Row{{"embedding": []any{float64(1), float64(2)}}})
	if err == nil {
		t.Fatal("expected invalid dimension error")
	}
	if !strings.Contains(err.Error(), `invalid dim`) {
		t.Fatalf("error = %q, want invalid dim context", err.Error())
	}
}

func TestColumnsFromRowsPreservesLargeInt64JSONNumber(t *testing.T) {
	field := &entity.Field{Name: "id", DataType: entity.FieldTypeInt64}
	row, err := decodeRow([]byte(`{"id":9223372036854775807}`))
	if err != nil {
		t.Fatalf("decode row: %v", err)
	}

	cols, err := columnsFromRows(&entity.Schema{Fields: []*entity.Field{field}}, []Row{row})
	if err != nil {
		t.Fatalf("columns from rows: %v", err)
	}

	got, err := cols[0].Get(0)
	if err != nil {
		t.Fatalf("get column value: %v", err)
	}
	if got != int64(math.MaxInt64) {
		t.Fatalf("column value = %v (%T), want %d", got, got, int64(math.MaxInt64))
	}
}
