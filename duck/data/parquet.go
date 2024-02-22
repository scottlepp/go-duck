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

func ToParquet(frames []*data.Frame, chunk int) (map[string]string, error) {
	dirs := map[string]string{}
	frameIndex := framesByRef(frames)
	for _, frameList := range frameIndex {

		dir, err := os.MkdirTemp("", "duck")
		if err != nil {
			return nil, err
		}

		for _, frame := range frameList {
			dirs[frame.RefID] = dir

			schema, err := MarshalArrow(frame)
			if err != nil {
				return nil, err
			}

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
					return nil, err
				}

				continue
			}

			raw, err := json.Marshal(data)
			if err != nil {
				return nil, err
			}

			_, _, err = write(dir, frame.RefID, schema, raw)
			if err != nil {
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

func framesByRef(frames []*data.Frame) map[string][]*data.Frame {
	byRef := map[string][]*data.Frame{}
	for _, f := range frames {
		fr := byRef[f.RefID]
		if fr == nil {
			refFrames := []*data.Frame{}
			byRef[f.RefID] = refFrames
		}
		byRef[f.RefID] = append(byRef[f.RefID], f)
	}
	return byRef
}
