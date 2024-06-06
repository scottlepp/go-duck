package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
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

			table, err := data.FrameToArrowTable(frame)
			if err != nil {
				logger.Error("failed to create arrow table", "error", err)
				return nil, err
			}
			// TODO... no need to create the table from data anymore,
			// BUT it means any modifications must be made before creating the arrow.Table

			schema := table.Schema()
			data := frameData(frame)

			if chunk > 0 {
				var wg sync.WaitGroup
				errCh := make(chan error, 10)
				// write files in chunks
				chunks := makeChunks(data, chunk)
				for i, chunk := range chunks {
					wg.Add(1)

					go func(chunk FrameData, idx int) error {
						defer wg.Done()
						raw, err := json.Marshal(chunk)
						if err != nil {
							logger.Error("failed to marshal chunk", "error", err)
							return err
						}
						name := fmt.Sprintf("%s%d", frame.RefID, idx)
						_, _, err = write(dir, name, schema, raw)
						if err != nil {
							logger.Error("failed to write parquet file", "error", err)
						}
						return err
					}(chunk, i)
				}

				go func() {
					wg.Wait()
					close(errCh)
				}()

				for err := range errCh {
					logger.Error("failed to write chunk", "error", err)
					return nil, err
				}

				continue
			}

			raw, err := json.Marshal(data)
			if err != nil {
				logger.Error("parquet failed to marshal frame data to raw data", "error", err)
				return nil, err
			}

			name := fmt.Sprintf("%s%d", frame.RefID, i)
			_, _, err = write(dir, name, schema, raw)
			if err != nil {
				logger.Error("parquet failed to write parquet file", "error", err)
				return nil, err
			}
		}
	}
	return dirs, nil
}

func frameData(frame *data.Frame) FrameData {
	data := FrameData{}
	for rowIdx := 0; rowIdx < frame.Rows(); rowIdx++ {
		row := map[string]any{}
		for colIdx, f := range frame.Fields {
			name := getFieldName(f)
			row[name] = frame.At(colIdx, rowIdx)
		}
		data = append(data, row)
	}
	return data
}

func getFieldName(field *data.Field) string {
	if field.Config != nil && field.Config.DisplayName != "" {
		return field.Config.DisplayName
	}
	return field.Name
}

func write(dir string, name string, schema *arrow.Schema, jsonData []byte) (string, string, error) {
	filename := path.Join(dir, name+".parquet")
	output, err := os.Create(filename)
	if err != nil {
		logger.Error("failed to create parquet file", "file", filename, "error", err)
		return "", "", err
	}

	defer output.Close()

	writerProps := parquet.NewWriterProperties()
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		logger.Error("failed to create parquet writer", "error", err)
		return "", "", err
	}
	r := bytes.NewReader(jsonData)
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, r)
	if err != nil {
		logger.Error("failed to create record from json", "error", err)
		return "", "", err
	}

	err = writer.Write(record)
	if err != nil {
		logger.Error("failed to write record", "error", err)
		return "", "", err
	}

	err = writer.Close()
	if err != nil {
		logger.Error("failed to close writer", "error", err)
		return dir, output.Name(), nil
	}
	return dir, output.Name(), nil
}

type FrameData []map[string]any

func makeChunks(xs FrameData, chunkSize int) []FrameData {
	if len(xs) == 0 {
		return nil
	}
	divided := make([]FrameData, (len(xs)+chunkSize-1)/chunkSize)
	prev := 0
	i := 0
	till := len(xs) - chunkSize
	for prev < till {
		next := prev + chunkSize
		divided[i] = xs[prev:next]
		prev = next
		i++
	}
	divided[i] = xs[prev:]
	return divided
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
