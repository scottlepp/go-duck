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
	if opts != nil && len(opts) > 0 {
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

func (d *DuckDB) QueryFramesInto(name string, query string, frames []*sdk.Frame, v any) (any, error) {
	res, err := d.QueryFrames(name, query, frames)
	if err != nil {
		return "", err
	}

	// if v is a frame then return a new frame with the results
	if f := isFrame(v); f != nil {
		var data []map[string]any
		err := json.Unmarshal([]byte(res), &data)
		if err != nil {
			return nil, err
		}
		resultsFrame, err := framestruct.ToDataFrame(name, data)
		if err != nil {
			return nil, err
		}

		f.Fields = resultsFrame.Fields
		f.Name = resultsFrame.Name
		f.Meta = resultsFrame.Meta
		f.RefID = resultsFrame.RefID

		return resultsFrame, nil
	}

	err = json.Unmarshal([]byte(res), v)
	if err != nil {
		return "", err
	}
	return v, nil
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

func isFrame(v any) *sdk.Frame {
	if f, ok := v.(*sdk.Frame); ok {
		return f
	}
	if f, ok := v.(sdk.Frame); ok {
		return &f
	}
	return nil
}
