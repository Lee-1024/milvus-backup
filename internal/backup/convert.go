package backup

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
)

func normalizeValue(v any) any {
	switch x := v.(type) {
	case []byte:
		return base64.StdEncoding.EncodeToString(x)
	case json.RawMessage:
		return json.RawMessage(x)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case entity.SparseEmbedding:
		out := make(map[string]float32, x.Len())
		for i := 0; i < x.Len(); i++ {
			pos, value, ok := x.Get(i)
			if ok {
				out[strconv.FormatUint(uint64(pos), 10)] = value
			}
		}
		return out
	default:
		return x
	}
}

func cloneSchema(in *entity.Schema, name string) *entity.Schema {
	out := entity.NewSchema().
		WithName(name).
		WithDescription(in.Description).
		WithAutoID(in.AutoID).
		WithDynamicFieldEnabled(in.EnableDynamicField)
	for _, f := range in.Fields {
		if f.IsDynamic {
			continue
		}
		cp := *f
		cp.ID = 0
		cp.TypeParams = copyMap(f.TypeParams)
		cp.IndexParams = copyMap(f.IndexParams)
		out.WithField(&cp)
	}
	for _, fn := range in.Functions {
		out.WithFunction(fn)
	}
	return out
}

func copyMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func columnsFromRows(schema *entity.Schema, rows []Row) ([]column.Column, error) {
	cols := make([]column.Column, 0, len(schema.Fields))
	functionOutputs := functionOutputFields(schema)
	for _, field := range schema.Fields {
		if field.AutoID || field.IsDynamic || functionOutputs[field.Name] {
			continue
		}
		col, err := columnFromRows(field, rows)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func functionOutputFields(schema *entity.Schema) map[string]bool {
	out := make(map[string]bool)
	for _, fn := range schema.Functions {
		for _, name := range fn.OutputFieldNames {
			out[name] = true
		}
	}
	return out
}

func columnFromRows(field *entity.Field, rows []Row) (column.Column, error) {
	name := field.Name
	switch field.DataType {
	case entity.FieldTypeBool:
		v, err := collect(rows, name, toBool)
		return column.NewColumnBool(name, v), err
	case entity.FieldTypeInt8:
		v, err := collect(rows, name, func(x any) (int8, error) { n, e := toInt64(x); return int8(n), e })
		return column.NewColumnInt8(name, v), err
	case entity.FieldTypeInt16:
		v, err := collect(rows, name, func(x any) (int16, error) { n, e := toInt64(x); return int16(n), e })
		return column.NewColumnInt16(name, v), err
	case entity.FieldTypeInt32:
		v, err := collect(rows, name, func(x any) (int32, error) { n, e := toInt64(x); return int32(n), e })
		return column.NewColumnInt32(name, v), err
	case entity.FieldTypeInt64:
		v, err := collect(rows, name, toInt64)
		return column.NewColumnInt64(name, v), err
	case entity.FieldTypeFloat:
		v, err := collect(rows, name, func(x any) (float32, error) { n, e := toFloat64(x); return float32(n), e })
		return column.NewColumnFloat(name, v), err
	case entity.FieldTypeDouble:
		v, err := collect(rows, name, toFloat64)
		return column.NewColumnDouble(name, v), err
	case entity.FieldTypeString:
		v, err := collect(rows, name, toString)
		return column.NewColumnString(name, v), err
	case entity.FieldTypeVarChar:
		v, err := collect(rows, name, toString)
		return column.NewColumnVarChar(name, v), err
	case entity.FieldTypeJSON:
		v, err := collect(rows, name, toJSONBytes)
		return column.NewColumnJSONBytes(name, v), err
	case entity.FieldTypeGeometry:
		v, err := collect(rows, name, toString)
		return column.NewColumnGeometryWKT(name, v), err
	case entity.FieldTypeFloatVector:
		v, err := collect(rows, name, toFloat32Slice)
		return column.NewColumnFloatVector(name, dim(field), v), err
	case entity.FieldTypeBinaryVector:
		v, err := collect(rows, name, toBytes)
		return column.NewColumnBinaryVector(name, dim(field), v), err
	case entity.FieldTypeFloat16Vector:
		v, err := collect(rows, name, toBytes)
		return column.NewColumnFloat16Vector(name, dim(field), v), err
	case entity.FieldTypeBFloat16Vector:
		v, err := collect(rows, name, toBytes)
		return column.NewColumnBFloat16Vector(name, dim(field), v), err
	case entity.FieldTypeInt8Vector:
		v, err := collect(rows, name, toInt8Slice)
		return column.NewColumnInt8Vector(name, dim(field), v), err
	case entity.FieldTypeSparseVector:
		v, err := collectSparseEmbeddings(rows, name)
		return column.NewColumnSparseVectors(name, v), err
	case entity.FieldTypeArray:
		return arrayColumnFromRows(field, rows)
	default:
		return nil, fmt.Errorf("field %s has unsupported type %s", name, field.DataType.Name())
	}
}

func arrayColumnFromRows(field *entity.Field, rows []Row) (column.Column, error) {
	name := field.Name
	switch field.ElementType {
	case entity.FieldTypeBool:
		v, err := collect(rows, name, toBoolSlice)
		return column.NewColumnBoolArray(name, v), err
	case entity.FieldTypeInt8:
		v, err := collect(rows, name, toInt8Slice)
		return column.NewColumnInt8Array(name, v), err
	case entity.FieldTypeInt16:
		v, err := collect(rows, name, toInt16Slice)
		return column.NewColumnInt16Array(name, v), err
	case entity.FieldTypeInt32:
		v, err := collect(rows, name, toInt32Slice)
		return column.NewColumnInt32Array(name, v), err
	case entity.FieldTypeInt64:
		v, err := collect(rows, name, toInt64Slice)
		return column.NewColumnInt64Array(name, v), err
	case entity.FieldTypeFloat:
		v, err := collect(rows, name, toFloat32Slice)
		return column.NewColumnFloatArray(name, v), err
	case entity.FieldTypeDouble:
		v, err := collect(rows, name, toFloat64Slice)
		return column.NewColumnDoubleArray(name, v), err
	case entity.FieldTypeVarChar, entity.FieldTypeString:
		v, err := collect(rows, name, toStringSlice)
		return column.NewColumnVarCharArray(name, v), err
	default:
		return nil, fmt.Errorf("array field %s has unsupported element type %s", name, field.ElementType.Name())
	}
}

func collect[T any](rows []Row, name string, conv func(any) (T, error)) ([]T, error) {
	out := make([]T, 0, len(rows))
	for i, row := range rows {
		v, ok := row[name]
		if !ok {
			var zero T
			out = append(out, zero)
			continue
		}
		typed, err := conv(v)
		if err != nil {
			return nil, fmt.Errorf("field %s row %d: %w", name, i, err)
		}
		out = append(out, typed)
	}
	return out, nil
}

func collectSparseEmbeddings(rows []Row, name string) ([]entity.SparseEmbedding, error) {
	out := make([]entity.SparseEmbedding, 0, len(rows))
	for i, row := range rows {
		v, ok := row[name]
		if !ok {
			v = nil
		}
		typed, err := toSparseEmbedding(v)
		if err != nil {
			return nil, fmt.Errorf("field %s row %d: %w", name, i, err)
		}
		out = append(out, typed)
	}
	return out, nil
}

func dim(field *entity.Field) int {
	n, _ := strconv.Atoi(field.TypeParams["dim"])
	return n
}

func toBool(v any) (bool, error) {
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expected bool, got %T", v)
	}
	return b, nil
}

func toString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", v)
	}
	return s, nil
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case float64:
		return int64(x), nil
	case int64:
		return x, nil
	case json.Number:
		return x.Int64()
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case json.Number:
		return x.Float64()
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}

func toBytes(v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("expected base64 string, got %T", v)
	}
	return base64.StdEncoding.DecodeString(s)
}

func toJSONBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case string:
		if b, err := base64.StdEncoding.DecodeString(x); err == nil {
			return b, nil
		}
		return []byte(x), nil
	default:
		return json.Marshal(x)
	}
}

func toSlice(v any) ([]any, error) {
	if v == nil {
		return []any{}, nil
	}
	in, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", v)
	}
	return in, nil
}

func toFloat32Slice(v any) ([]float32, error) {
	in, err := toSlice(v)
	if err != nil {
		return nil, err
	}
	out := make([]float32, len(in))
	for i, x := range in {
		n, err := toFloat64(x)
		if err != nil {
			return nil, err
		}
		out[i] = float32(n)
	}
	return out, nil
}

func toFloat64Slice(v any) ([]float64, error) {
	in, err := toSlice(v)
	if err != nil {
		return nil, err
	}
	out := make([]float64, len(in))
	for i, x := range in {
		out[i], err = toFloat64(x)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func toInt8Slice(v any) ([]int8, error) {
	in, err := toSlice(v)
	if err != nil {
		return nil, err
	}
	out := make([]int8, len(in))
	for i, x := range in {
		n, err := toInt64(x)
		if err != nil {
			return nil, err
		}
		out[i] = int8(n)
	}
	return out, nil
}

func toInt16Slice(v any) ([]int16, error) {
	in, err := toSlice(v)
	if err != nil {
		return nil, err
	}
	out := make([]int16, len(in))
	for i, x := range in {
		n, err := toInt64(x)
		if err != nil {
			return nil, err
		}
		out[i] = int16(n)
	}
	return out, nil
}

func toInt32Slice(v any) ([]int32, error) {
	in, err := toSlice(v)
	if err != nil {
		return nil, err
	}
	out := make([]int32, len(in))
	for i, x := range in {
		n, err := toInt64(x)
		if err != nil {
			return nil, err
		}
		out[i] = int32(n)
	}
	return out, nil
}

func toInt64Slice(v any) ([]int64, error) {
	in, err := toSlice(v)
	if err != nil {
		return nil, err
	}
	out := make([]int64, len(in))
	for i, x := range in {
		out[i], err = toInt64(x)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func toBoolSlice(v any) ([]bool, error) {
	in, err := toSlice(v)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(in))
	for i, x := range in {
		out[i], err = toBool(x)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func toStringSlice(v any) ([]string, error) {
	in, err := toSlice(v)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(in))
	for i, x := range in {
		out[i], err = toString(x)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func toSparseEmbedding(v any) (entity.SparseEmbedding, error) {
	switch x := v.(type) {
	case nil:
		return entity.NewSliceSparseEmbedding(nil, nil)
	case map[string]any:
		positions := make([]uint32, 0, len(x))
		values := make([]float32, 0, len(x))
		for key, value := range x {
			pos, err := strconv.ParseUint(key, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid sparse vector index %q: %w", key, err)
			}
			n, err := toFloat64(value)
			if err != nil {
				return nil, err
			}
			positions = append(positions, uint32(pos))
			values = append(values, float32(n))
		}
		return entity.NewSliceSparseEmbedding(positions, values)
	case map[string]float32:
		positions := make([]uint32, 0, len(x))
		values := make([]float32, 0, len(x))
		for key, value := range x {
			pos, err := strconv.ParseUint(key, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid sparse vector index %q: %w", key, err)
			}
			positions = append(positions, uint32(pos))
			values = append(values, value)
		}
		return entity.NewSliceSparseEmbedding(positions, values)
	case string:
		b, err := base64.StdEncoding.DecodeString(x)
		if err != nil {
			return nil, fmt.Errorf("expected sparse vector object or base64 bytes, got string: %w", err)
		}
		return entity.DeserializeSliceSparseEmbedding(b)
	default:
		return nil, fmt.Errorf("expected sparse vector object, got %T", v)
	}
}
