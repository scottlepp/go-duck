package duck

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"

	sdk "github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/framestruct"
	"github.com/scottlepp/go-duck/duck/data"
)

type DuckDB struct {
	Name   string
	Mode   string
	Format string
	Chunk  int
}

type Opts struct {
	Mode   string
	Format string
	Chunk  int
}

const newline = "\n"

// NewInMemoryDB creates a new in-memory DuckDB
func NewInMemoryDB(opts ...Opts) DuckDB {
	return NewDuckDB("", opts...)
}

// NewDuckDB creates a new DuckDB
func NewDuckDB(name string, opts ...Opts) DuckDB {
	if len(opts) > 0 {
		return DuckDB{
			Name:   name,
			Mode:   defaultString(opts[0].Mode, "json"),
			Format: defaultString(opts[0].Format, "parquet"),
			Chunk:  defaultInt(opts[0].Chunk, 0),
		}
	}
	return DuckDB{
		Name:   name,
		Mode:   "json",
		Format: "parquet",
		Chunk:  0,
	}
}

// RunCommands runs a series of of sql commands against duckdb
func (d *DuckDB) RunCommands(commands []string) (string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	var b bytes.Buffer
	b.Write([]byte(d.mode()))
	for _, c := range commands {
		cmd := fmt.Sprintf("%s %s", c, newline)
		b.Write([]byte(cmd))
	}

	cmd := exec.Command("duckdb", d.Name)
	cmd.Stdin = &b
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		message := err.Error() + stderr.String()
		return "", errors.New(message)
	}
	if stderr.String() != "" {
		return "", errors.New(stderr.String())
	}

	return stdout.String(), nil
}

func (d *DuckDB) mode() string {
	m := d.Mode
	if m == "" {
		m = "json"
	}
	return fmt.Sprintf(".mode %s \n", m)
}

// Query runs a query against the database. For Databases that are NOT in-memory.
func (d *DuckDB) Query(query string) (string, error) {
	return d.RunCommands([]string{query})
}

// QueryFrame will load a dataframe into a view named RefID, and run the query against that view
func (d *DuckDB) QueryFrames(name string, query string, frames []*sdk.Frame) (string, error) {
	dirs, err := data.ToParquet(frames, d.Chunk)
	if err != nil {
		return "", err
	}

	defer func() {
		for _, dir := range dirs {
			err := os.RemoveAll(dir)
			if err != nil {
				fmt.Println("failed to remove parquet files")
			}
		}
	}()

	commands := []string{}
	created := map[string]bool{}
	for _, frame := range frames {
		if created[frame.RefID] {
			continue
		}
		cmd := fmt.Sprintf("CREATE VIEW %s AS (SELECT * from '%s/*.parquet');", frame.RefID, dirs[frame.RefID])
		commands = append(commands, cmd)
		created[frame.RefID] = true
	}

	commands = append(commands, query)
	res, err := d.RunCommands(commands)
	if err != nil {
		return "", err
	}
	return res, nil
}

func (d *DuckDB) QueryFramesInto(name string, query string, frames []*sdk.Frame, f *sdk.Frame) error {
	res, err := d.QueryFrames(name, query, frames)
	if err != nil {
		return err
	}

	return resultsToFrame(name, res, f, frames)
}

// Destroy will remove database files created by duckdb
func (d *DuckDB) Destroy() error {
	if d.Name != "" {
		return os.Remove(d.Name)
	}
	return nil
}

func defaultString(val string, dflt string) string {
	if val == "" {
		return dflt
	}
	return val
}

func defaultInt(val int, dflt int) int {
	if val == 0 {
		return dflt
	}
	return val
}

func resultsToFrame(name string, res string, f *sdk.Frame, frames []*sdk.Frame) error {
	var results []map[string]any
	err := json.Unmarshal([]byte(res), &results)
	if err != nil {
		return err
	}
	converters := data.Converters(frames)
	resultsFrame, err := framestruct.ToDataFrame(name, results, converters...)
	if err != nil {
		return err
	}

	f.Fields = resultsFrame.Fields
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
