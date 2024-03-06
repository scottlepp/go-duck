package data

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// keys added to arrow field metadata
const metadataKeyName = "name"     // standard property
const metadataKeyConfig = "config" // FieldConfig serialized as JSON
const metadataKeyLabels = "labels" // labels serialized as JSON
const metadataKeyRefID = "refId"   // added to the table metadata

// MarshalArrow converts the Frame to an arrow table and returns a byte
// representation of that table.
// All fields of a Frame must be of the same length or an error is returned.
func MarshalArrow(f *data.Frame) (*arrow.Schema, error) {
	if _, err := f.RowLen(); err != nil {
		return nil, err
	}

	arrowFields, err := buildArrowFields(f)
	if err != nil {
		return nil, err
	}

	schema, err := buildArrowSchema(f, arrowFields)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

// buildArrowFields builds Arrow field definitions from a Frame.
func buildArrowFields(f *data.Frame) ([]arrow.Field, error) {
	arrowFields := make([]arrow.Field, len(f.Fields))

	for i, field := range f.Fields {
		t, nullable, err := fieldToArrow(field)
		if err != nil {
			return nil, err
		}
		fieldMeta := map[string]string{}

		if field.Labels != nil {
			if fieldMeta[metadataKeyLabels], err = toJSONString(field.Labels); err != nil {
				return nil, err
			}
		}

		if field.Config != nil {
			str, err := toJSONString(field.Config)
			if err != nil {
				return nil, err
			}
			fieldMeta[metadataKeyConfig] = str
		}

		arrowFields[i] = arrow.Field{
			Name:     field.Name,
			Type:     t,
			Metadata: arrow.MetadataFrom(fieldMeta),
			Nullable: nullable,
		}
	}

	return arrowFields, nil
}

// buildArrowSchema builds an Arrow schema for a Frame.
func buildArrowSchema(f *data.Frame, fs []arrow.Field) (*arrow.Schema, error) {
	tableMetaMap := map[string]string{
		metadataKeyName:  f.Name,
		metadataKeyRefID: f.RefID,
	}
	if f.Meta != nil {
		str, err := toJSONString(f.Meta)
		if err != nil {
			return nil, err
		}
		tableMetaMap["meta"] = str
	}
	tableMeta := arrow.MetadataFrom(tableMetaMap)

	return arrow.NewSchema(fs, &tableMeta), nil
}

// fieldToArrow returns the corresponding Arrow primitive type and nullable property to the fields'
// Vector primitives.
// nolint:gocyclo
func fieldToArrow(f *data.Field) (arrow.DataType, bool, error) {
	switch f.Type() {
	case data.FieldTypeString:
		return &arrow.StringType{}, false, nil
	case data.FieldTypeNullableString:
		return &arrow.StringType{}, true, nil

	// Ints
	case data.FieldTypeInt8:
		return &arrow.Int8Type{}, false, nil
	case data.FieldTypeNullableInt8:
		return &arrow.Int8Type{}, true, nil

	case data.FieldTypeInt16:
		return &arrow.Int16Type{}, false, nil
	case data.FieldTypeNullableInt16:
		return &arrow.Int16Type{}, true, nil

	case data.FieldTypeInt32:
		return &arrow.Int32Type{}, false, nil
	case data.FieldTypeNullableInt32:
		return &arrow.Int32Type{}, true, nil

	case data.FieldTypeInt64:
		return &arrow.Int64Type{}, false, nil
	case data.FieldTypeNullableInt64:
		return &arrow.Int64Type{}, true, nil

	// Uints
	case data.FieldTypeUint8:
		return &arrow.Uint8Type{}, false, nil
	case data.FieldTypeNullableUint8:
		return &arrow.Uint8Type{}, true, nil

	case data.FieldTypeUint16, data.FieldTypeEnum:
		return &arrow.Uint16Type{}, false, nil
	case data.FieldTypeNullableUint16, data.FieldTypeNullableEnum:
		return &arrow.Uint16Type{}, true, nil

	case data.FieldTypeUint32:
		return &arrow.Uint32Type{}, false, nil
	case data.FieldTypeNullableUint32:
		return &arrow.Uint32Type{}, true, nil

	case data.FieldTypeUint64:
		return &arrow.Uint64Type{}, false, nil
	case data.FieldTypeNullableUint64:
		return &arrow.Uint64Type{}, true, nil

	case data.FieldTypeFloat32:
		return &arrow.Float32Type{}, false, nil
	case data.FieldTypeNullableFloat32:
		return &arrow.Float32Type{}, true, nil

	case data.FieldTypeFloat64:
		return &arrow.Float64Type{}, false, nil
	case data.FieldTypeNullableFloat64:
		return &arrow.Float64Type{}, true, nil

	case data.FieldTypeBool:
		return &arrow.BooleanType{}, false, nil
	case data.FieldTypeNullableBool:
		return &arrow.BooleanType{}, true, nil

	case data.FieldTypeTime:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, false, nil
	case data.FieldTypeNullableTime:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, true, nil

	case data.FieldTypeJSON:
		return &arrow.BinaryType{}, false, nil
	case data.FieldTypeNullableJSON:
		return &arrow.BinaryType{}, true, nil

	default:
		return nil, false, fmt.Errorf("unsupported type for conversion to arrow: %T", f.Type())
	}
}

// ToJSONString calls json.Marshal on val and returns it as a string. An
// error is returned if json.Marshal errors.
func toJSONString(val interface{}) (string, error) {
	b, err := json.Marshal(val)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
