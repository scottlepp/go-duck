package duck

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	sdk "github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/framestruct"
	"github.com/hairyhenderson/go-which"
	"github.com/iancoleman/orderedmap"
	"github.com/jeremywohl/flatten"
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
	docker        bool
	image         string
}

type Opts struct {
	Mode          string
	Format        string
	Chunk         int
	Exe           string
	CacheDuration int
	Docker        bool
	Image         string
}

const newline = "\n"
const duckdbImage = "datacatering/duckdb:v1.0.0"

var tempDir = getTempDir()

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
		db.image = duckdbImage
		if opt.Image != "" {
			db.image = opt.Image
		}
		db.docker = opt.Docker
	}

	// Find the executable if it is not configured
	if db.exe == "" && !db.docker {
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
	return d.runCommands(commands)
}

// Query runs a query against the database. For Databases that are NOT in-memory.
func (d *DuckDB) Query(query string) (string, error) {
	return d.RunCommands([]string{query})
}

// QueryFrame will load a dataframe into a view named RefID, and run the query against that view
func (d *DuckDB) QueryFrames(name string, query string, frames []*sdk.Frame) (string, bool, error) {
	err := d.validate(query)
	if err != nil {
		return "", false, err
	}
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

func (d *DuckDB) QueryFramesToFrames(name string, query string, frames []*sdk.Frame) (*sdk.Frame, error) {
	err := d.validate(query)
	if err != nil {
		return nil, err
	}

	f := &sdk.Frame{}
	res, cached, err := d.QueryFrames(name, query, frames)
	if err != nil {
		return nil, err
	}

	err = resultsToFrame(name, res, f, frames)
	if err != nil {
		return nil, err
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
	return f, nil
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

	data.ConvertDateFields(results)

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

	kind := f.TimeSeriesSchema().Type
	if kind == sdk.TimeSeriesTypeLong {
		fillMode := &sdk.FillMissing{Mode: sdk.FillModeNull}
		frame, err := sdk.LongToWide(f, fillMode)
		if err != nil {
			logger.Warn("could not convert frame long to wide", "error", err)
			return nil
		}
		f.Fields = frame.Fields
		f.Meta = frame.Meta
		return nil
	}

	if kind == sdk.TimeSeriesTypeWide {
		if f.Meta == nil {
			f.Meta = &sdk.FrameMeta{}
		}
		f.Meta.Type = sdk.FrameTypeTimeSeriesWide
	}

	// TODO - appending to field names for now
	// applyLabels(*resultsFrame, frames)

	return nil
}

func (d *DuckDB) runCommands(commands []string) (string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	var b bytes.Buffer
	b.Write([]byte(fmt.Sprintf(".mode %s %s", d.mode, newline)))
	for _, c := range commands {
		cmd := fmt.Sprintf("%s %s", c, newline)
		b.Write([]byte(cmd))
	}

	var cmd *exec.Cmd
	if d.docker {
		volume := fmt.Sprintf("%s:%s", tempDir, tempDir)
		logger.Debug("running command in docker", "volume", volume, "image", duckdbImage)
		cmd = exec.Command("docker", "run", "-i", "-v", volume, duckdbImage)
	} else {
		cmd = exec.Command(d.exe, d.Name)
	}
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

func getTempDir() string {
	temp := os.Getenv("TMPDIR")
	if temp == "" {
		temp = "/tmp"
	}
	return temp
}

const (
	TABLE_NAME    = "table_name"
	ERROR         = ".error"
	ERROR_MESSAGE = ".error_message"
)

func (d *DuckDB) validate(rawSQL string) error {
	rawSQL = strings.Replace(rawSQL, "'", "''", -1)
	cmd := fmt.Sprintf("SELECT json_serialize_sql('%s')", rawSQL)
	ret, err := d.RunCommands([]string{cmd})
	if err != nil {
		logger.Error("error validating sql", "error", err.Error(), "sql", rawSQL, "cmd", cmd)
		return fmt.Errorf("error validating sql: %s", err.Error())
	}

	result := []map[string]any{}
	err = json.Unmarshal([]byte(ret), &result)
	if err != nil {
		logger.Error("error converting json sql to ast", "error", err.Error(), "ret", ret)
		return fmt.Errorf("error converting json to ast: %s", err.Error())
	}

	if len(result) == 0 {
		logger.Error("no ast returned", "ret", ret)
	}

	var ast map[string]any
	for _, v := range result[0] {
		validAst, ok := v.(map[string]any)
		if !ok {
			logger.Error("invalid sql", "sql", ret)
			return fmt.Errorf("invalid sql: %s", ret)
		}
		ast = validAst
		break
	}

	errMsg := ast["error"]
	if errMsg != nil {
		errMsgBool, ok := errMsg.(bool)
		if !ok {
			logger.Error("error in ast", "error", ret)
			return fmt.Errorf("error in ast: %v", ret)
		}
		if errMsgBool {
			logger.Error("error in ast", "error", ret)
			return fmt.Errorf("error in ast: %v", ret)
		}
	}

	statements := ast["statements"]
	if statements == nil {
		logger.Error("no statements in ast", "ast", ast)
		return fmt.Errorf("no statements in ast: %v", ast)
	}

	flat, err := flatten.Flatten(ast, "", flatten.DotStyle)
	if err != nil {
		logger.Error("error flattening ast", "error", err.Error(), "ast", ast)
		return fmt.Errorf("error flattening ast: %s", err.Error())
	}

	for k, v := range flat {
		if strings.HasSuffix(k, ERROR) {
			v, ok := v.(bool)
			if ok && v {
				logger.Error("error in sql", "error", k)
				return fmt.Errorf("error flattening ast: %s", k)
			}
		}
		if strings.Contains(k, "from_table.function.function_name") {
			logger.Error("function not allowed", "function", v)
			return fmt.Errorf("function not allowed: %s", v)
		}
		if strings.HasSuffix(k, "from_table.table_name") {
			v, ok := v.(string)
			if ok && strings.Contains(v, ".") {
				logger.Error("table names with . not allowed", "table", v)
				return fmt.Errorf("table names with . not allowed: %s", v)
			}
		}
	}

	return nil
}
