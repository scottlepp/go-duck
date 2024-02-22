package main

import (
	"fmt"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/stretchr/testify/assert"
)

func TestCommands(t *testing.T) {
	db := NewInMemoryDB()

	commands := []string{
		"CREATE TABLE t1 (i INTEGER, j INTEGER);",
		"INSERT INTO t1 VALUES (1, 5);",
		"SELECT * from t1;",
	}
	res, err := db.RunCommands(commands)
	if err != nil {
		t.Fail()
		return
	}
	assert.Contains(t, res, `[{"i":1,"j":5}]`)
}

func TestQuery(t *testing.T) {
	db := NewDuckDB("foo")

	commands := []string{
		"CREATE TABLE t1 (i INTEGER, j INTEGER);",
		"INSERT INTO t1 VALUES (1, 5);",
	}
	_, err := db.RunCommands(commands)
	assert.Nil(t, err)

	res, err := db.Query("SELECT * from t1;")
	assert.Nil(t, err)
	assert.Contains(t, res, `[{"i":1,"j":5}]`)

	err = db.Destroy()
	assert.Nil(t, err)
}

func TestQueryFrame(t *testing.T) {
	db := NewInMemoryDB()

	var values = []string{"test"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"

	res, err := db.QueryFrame("foo", "select * from foo", frame)
	assert.Nil(t, err)

	assert.Contains(t, res, `[{"value":"test"}]`)
}

func TestQueryFrameChunks(t *testing.T) {
	opts := Opts{
		Chunk: 3,
	}
	db := NewInMemoryDB(opts)

	var values = []string{"test", "test", "test", "test", "test", "test2"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"

	res, err := db.QueryFrame("foo", "select * from foo", frame)
	assert.Nil(t, err)

	assert.Contains(t, res, `test2`)
}

func TestQueryFrameInto(t *testing.T) {
	db := NewInMemoryDB()

	var values = []string{"test"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"

	model := []map[string]any{}
	_, err := db.QueryFrameInto("foo", "select * from foo", frame, &model)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(model))
	raw := fmt.Sprintf("%s", model)
	assert.Contains(t, raw, "test")
}
