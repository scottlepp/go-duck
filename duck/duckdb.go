package duck

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	sdk "github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/framestruct"
	"github.com/hairyhenderson/go-which"
	"github.com/iancoleman/orderedmap"
	"github.com/scottlepp/go-duck/duck/data"
)

var logger = log.DefaultLogger

type Dirs map[string]string

type DuckDB struct {
	Name          string
	mode          string
	format        string
	exe           string
	chunk         int
	cacheDuration int
	cache         cache
}

type Opts struct {
	Mode          string
	Format        string
	Chunk         int
	Exe           string
	CacheDuration int
}

const newline = "\n"

// NewInMemoryDB creates a new in-memory DuckDB
func NewInMemoryDB(opts ...Opts) *DuckDB {
	return NewDuckDB("", opts...)
}

// NewDuckDB creates a new DuckDB
func NewDuckDB(name string, opts ...Opts) *DuckDB {
	db := DuckDB{
		Name:   name,
		mode:   "json",
		format: "parquet",
	}
	for _, opt := range opts {
		if opt.Mode != "" {
			db.mode = opt.Mode
		}
		if opt.Format != "" {
			db.format = opt.Format
		}
		if opt.Exe != "" {
			db.exe = opt.Exe
		}
		if opt.Chunk > 0 {
			db.chunk = opt.Chunk
		}
		if opt.CacheDuration > 0 {
			db.cacheDuration = opt.CacheDuration
		}
	}

	// Find the executable if it is not configured
	if db.exe == "" {
		db.exe = which.Which("duckdb")
		if db.exe == "" {
			db.exe = "/usr/local/bin/duckdb"
		}
	}
	db.cache = cache{}
	return &db
}

// RunCommands runs a series of of sql commands against duckdb
func (d *DuckDB) RunCommands(commands []string) (string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	var b bytes.Buffer
	b.Write([]byte(fmt.Sprintf(".mode %s %s", d.mode, newline)))
	for _, c := range commands {
		cmd := fmt.Sprintf("%s %s", c, newline)
		b.Write([]byte(cmd))
	}

	cmd := exec.Command(d.exe, d.Name)
	cmd.Stdin = &b
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		message := err.Error() + stderr.String()
		logger.Error("error running command", "cmd", b.String(), "message", message, "error", err)
		return "", errors.New(message)
	}
	if stderr.String() != "" {
		logger.Error("error running command", "cmd", b.String(), "error", stderr.String())
		return "", errors.New(stderr.String())
	}

	return stdout.String(), nil
}

// Query runs a query against the database. For Databases that are NOT in-memory.
func (d *DuckDB) Query(query string) (string, error) {
	return d.RunCommands([]string{query})
}

// QueryFrame will load a dataframe into a view named RefID, and run the query against that view
func (d *DuckDB) QueryFrames(name string, query string, frames []*sdk.Frame) (string, bool, error) {
	data := FrameData{
		cacheDuration: d.cacheDuration,
		cache:         &d.cache,
		db:            d,
	}

	return data.Query(name, query, frames)
}

func wipe(dirs map[string]string) {
	for _, dir := range dirs {
		err := os.RemoveAll(dir)
		if err != nil {
			logger.Error("failed to remove parquet files", "error", err)
		}
	}
}

func (d *DuckDB) QueryFramesInto(name string, query string, frames []*sdk.Frame, f *sdk.Frame) error {
	res, cached, err := d.QueryFrames(name, query, frames)
	if err != nil {
		return err
	}

	err = resultsToFrame(name, res, f, frames)
	if err != nil {
		return err
	}
	if cached {
		for _, frame := range frames {
			if frame.Meta == nil {
				frame.Meta = &sdk.FrameMeta{}
			}
			notice := sdk.Notice{
				Severity: sdk.NoticeSeverityInfo,
				Text:     "Data retrieved from cache",
			}
			frame.Meta.Notices = append(frame.Meta.Notices, notice)
		}
	}
	return nil
}

// Destroy will remove database files created by duckdb
func (d *DuckDB) Destroy() error {
	if d.Name != "" {
		return os.Remove(d.Name)
	}
	return nil
}

func resultsToFrame(name string, res string, f *sdk.Frame, frames []*sdk.Frame) error {
	if res == "" {
		return nil
	}
	var results []map[string]any
	err := json.Unmarshal([]byte(res), &results)
	if err != nil {
		logger.Error("error unmarshalling results", "error", err)
		return err
	}
	converters := data.Converters(frames)
	resultsFrame, err := framestruct.ToDataFrame(name, results, converters...)

	if err != nil {
		logger.Error("error converting results to frame", "error", err)
		return err
	}

	// Order the fields in the same order as the source frame:
	// Build a slice of ordered keys

	var orderedKeys []string
	var temp []orderedmap.OrderedMap
	err = json.Unmarshal([]byte(res), &temp)
	if err == nil {
		orderedKeys = temp[0].Keys()
	}

	// Create a map of column names to indexes
	columnIndex := make(map[string]int)
	for i, field := range resultsFrame.Fields {
		columnIndex[field.Name] = i
	}
	// Add columns to the DataFrame
	for _, key := range orderedKeys {
		i := columnIndex[key]
		f.Fields = append(f.Fields, resultsFrame.Fields[i])
	}

	f.Name = resultsFrame.Name
	f.Meta = resultsFrame.Meta
	f.RefID = resultsFrame.RefID

	// TODO - appending to field names for now
	// applyLabels(*resultsFrame, frames)

	return nil
}

// TODO

// func applyLabels(resultsFrame sdk.Frame, sourceFrames []*sdk.Frame) {
// 	for _, fld := range resultsFrame.Fields {
// 		for _, f := range sourceFrames {
// 			srcField := find(f, fld)
// 			if srcField != nil {
// 				fld.Labels = srcField.Labels
// 				break
// 			}
// 		}
// 	}
// }

// func find(f *sdk.Frame, fld *sdk.Field) *sdk.Field {
// 	for _, sfld := range f.Fields {
// 		if sfld.Name == fld.Name {
// 			return sfld
// 		}
// 	}
// 	return nil
// }
