package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func ToParquet(frame *data.Frame, chunk int) (string, string, error) {
	schema, err := MarshalArrow(frame)
	if err != nil {
		return "", "", err
	}

	data := frameData(frame)

	dir, err := os.MkdirTemp("", "duck")
	if err != nil {
		return "", "", err
	}

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
					return err
				}
				name := fmt.Sprintf("%s%d", frame.RefID, idx)
				_, _, err = write(dir, name, schema, raw)
				return err
			}(chunk, i)
		}

		go func() {
			wg.Wait()
			close(errCh)
		}()

		for err := range errCh {
			return "", "", err
		}

		return dir, "", nil
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return "", "", err
	}

	return write(dir, frame.RefID, schema, raw)
}

func frameData(frame *data.Frame) FrameData {
	data := FrameData{}
	for rowIdx := 0; rowIdx < frame.Rows(); rowIdx++ {
		row := map[string]any{}
		for colIdx, f := range frame.Fields {
			row[f.Name] = frame.At(colIdx, rowIdx)
		}
		data = append(data, row)
	}
	return data
}

func write(dir string, name string, schema *arrow.Schema, jsonData []byte) (string, string, error) {
	filename := path.Join(dir, name+".parquet")
	output, err := os.Create(filename)
	if err != nil {
		return "", "", err
	}

	defer output.Close()

	writerProps := parquet.NewWriterProperties()
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, pqarrow.DefaultWriterProps())

	r := bytes.NewReader(jsonData)
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, r)
	if err != nil {
		return "", "", err
	}

	err = writer.Write(record)
	if err != nil {
		return "", "", err
	}

	err = writer.Close()
	if err != nil {
		fmt.Println("failed to close writer")
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
