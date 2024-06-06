package data

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

var logger = log.DefaultLogger

func ToParquet(frames []*data.Frame, chunk int) (map[string]string, error) {
	dirs := map[string]string{}
	frameIndex := framesByRef(frames)

	// TODO - appending lables to fields for now
	// need to return multiple frames instead
	// for _, f := range frames {
	// 	for _, fld := range f.Fields {
	// 		if len(fld.Labels) > 0 {
	// 			lbls := fld.Labels.String()
	// 			fld.Name = fmt.Sprintf("%s %s", fld.Name, lbls)
	// 		}
	// 	}
	// }
	writerProps := parquet.NewWriterProperties()
	SIZELEN := int64(1024 * 1024)

	for _, frameList := range frameIndex {

		labelsToFields(frameList)

		dir, err := os.MkdirTemp("", "duck")
		if err != nil {
			logger.Error("failed to create temp dir", "error", err)
			return nil, err
		}

		mergeFrames(frameList)
		for i, frame := range frameList {
			dirs[frame.RefID] = dir

			// Mutate the name to match display name
			// OK, because framesByRef already clones the field?
			for _, f := range frame.Fields {
				if f.Config != nil && f.Config.DisplayName != "" {
					f.Name = f.Config.DisplayName
					f.Config.DisplayName = ""
				}
			}

			table, err := data.FrameToArrowTable(frame)
			if err != nil {
				logger.Error("failed to create arrow table", "error", err)
				return nil, err
			}
			defer table.Release()

			name := fmt.Sprintf("%s%d", frame.RefID, i)
			filename := path.Join(dir, name+".parquet")
			output, err := os.Create(filename)
			if err != nil {
				logger.Error("failed to create parquet file", "file", filename, "error", err)
				return nil, err
			}
			defer output.Close()

			err = pqarrow.WriteTable(table, output, SIZELEN, writerProps, pqarrow.DefaultWriterProps())
			if err != nil {
				logger.Error("error writing parquet", "error", err)
				return nil, err
			}
		}
	}
	return dirs, nil
}

func framesByRef(frames []*data.Frame) map[string][]*data.Frame {
	byRef := map[string][]*data.Frame{}
	for _, f := range frames {
		fr := byRef[f.RefID]
		if fr == nil {
			refFrames := []*data.Frame{}
			byRef[f.RefID] = refFrames
		}
		byRef[f.RefID] = append(byRef[f.RefID], clone(f))
	}
	return byRef
}

func clone(f *data.Frame) *data.Frame {
	copy := data.NewFrame(f.Name, f.Fields...)
	copy.RefID = f.RefID
	copy.Meta = f.Meta
	return copy
}

func mergeFrames(frames []*data.Frame) {
	fields := map[string]*data.Field{}
	for _, f := range frames {
		for _, fld := range f.Fields {
			fields[fld.Name] = fld
		}
	}
	for _, fld := range fields {
		for _, f := range frames {
			found := false
			for _, fld2 := range f.Fields {
				if fld2.Name == fld.Name {
					found = true
					break
				}
			}
			if !found {
				makeArray := maker[fld.Type()]
				arr := makeArray(f.Rows())
				nullField := data.NewField(fld.Name, fld.Labels, arr)
				f.Fields = append(f.Fields, nullField)
			}
		}
	}
}

var maker = map[data.FieldType]func(length int) any{
	data.FieldTypeFloat64:         func(length int) any { return makeArray[float64](length) },
	data.FieldTypeFloat32:         func(length int) any { return makeArray[float32](length) },
	data.FieldTypeInt16:           func(length int) any { return makeArray[int16](length) },
	data.FieldTypeInt64:           func(length int) any { return makeArray[int64](length) },
	data.FieldTypeInt8:            func(length int) any { return makeArray[int8](length) },
	data.FieldTypeUint8:           func(length int) any { return makeArray[uint8](length) },
	data.FieldTypeUint16:          func(length int) any { return makeArray[uint16](length) },
	data.FieldTypeUint32:          func(length int) any { return makeArray[uint32](length) },
	data.FieldTypeUint64:          func(length int) any { return makeArray[uint64](length) },
	data.FieldTypeNullableFloat64: func(length int) any { return makeArray[*float64](length) },
	data.FieldTypeNullableFloat32: func(length int) any { return makeArray[*float32](length) },
	data.FieldTypeNullableInt16:   func(length int) any { return makeArray[*int16](length) },
	data.FieldTypeNullableInt64:   func(length int) any { return makeArray[*int64](length) },
	data.FieldTypeNullableInt8:    func(length int) any { return makeArray[*int8](length) },
	data.FieldTypeNullableUint8:   func(length int) any { return makeArray[*uint8](length) },
	data.FieldTypeNullableUint16:  func(length int) any { return makeArray[*uint16](length) },
	data.FieldTypeNullableUint32:  func(length int) any { return makeArray[*uint32](length) },
	data.FieldTypeNullableUint64:  func(length int) any { return makeArray[*uint64](length) },
	data.FieldTypeString:          func(length int) any { return makeArray[string](length) },
	data.FieldTypeNullableString:  func(length int) any { return makeArray[*string](length) },
	data.FieldTypeTime:            func(length int) any { return makeArray[time.Time](length) },
	data.FieldTypeNullableTime:    func(length int) any { return makeArray[*time.Time](length) },
}

func makeArray[T any](length int) []T {
	return make([]T, length)
}

func labelsToFields(frames []*data.Frame) {
	for _, f := range frames {
		fields := []*data.Field{}
		for _, fld := range f.Fields {
			if fld.Labels != nil {
				for lbl, val := range fld.Labels {
					newFld := newField(lbl, val, f.Rows())
					fields = append(fields, newFld)
				}
			}
		}
		f.Fields = append(f.Fields, fields...)
	}
}

func newField(name string, val string, size int) *data.Field {
	values := make([]string, size)
	newField := data.NewField(name, nil, values)
	for i := 0; i < size; i++ {
		newField.Set(i, val)
	}
	return newField
}
