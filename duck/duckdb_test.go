package duck

import (
	"fmt"
	"testing"
	"time"

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
	frames := []*data.Frame{frame}

	res, err := db.QueryFrames("foo", "select * from foo", frames)
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
	frames := []*data.Frame{frame}

	res, err := db.QueryFrames("foo", "select * from foo", frames)
	assert.Nil(t, err)

	assert.Contains(t, res, `test2`)
}

func TestQueryFrameInto(t *testing.T) {
	db := NewInMemoryDB()

	var values = []string{"test"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"
	frames := []*data.Frame{frame}

	model := []map[string]any{}
	_, err := db.QueryFramesInto("foo", "select * from foo", frames, &model)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(model))
	raw := fmt.Sprintf("%s", model)
	assert.Contains(t, raw, "test")
}

func TestQueryFrameIntoFrame(t *testing.T) {
	db := NewInMemoryDB()

	var values = []string{"test"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"

	var values2 = []string{"foo"}
	frame2 := data.NewFrame("foo", data.NewField("value", nil, values2))
	frame2.RefID = "foo"

	frames := []*data.Frame{frame, frame2}

	model := &data.Frame{}
	_, err := db.QueryFramesInto("foo", "select * from foo", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 1, model.Rows())
}

func TestMultiFrame(t *testing.T) {
	db := NewInMemoryDB()

	var values = []string{"test"}
	frame := data.NewFrame("foo", data.NewField("value1", nil, values))
	frame.RefID = "foo"

	var values2 = []string{"foo"}
	frame2 := data.NewFrame("foo", data.NewField("value2", nil, values2))
	frame2.RefID = "foo"

	frames := []*data.Frame{frame, frame2}

	model := &data.Frame{}
	_, err := db.QueryFramesInto("foo", "select * from foo", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 2, model.Rows())
	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)
}

func TestMultiFrame2(t *testing.T) {
	db := NewInMemoryDB()

	f := new(float64)
	*f = 12345

	var values = []*float64{f}
	frame := data.NewFrame("foo", data.NewField("value1", nil, values))
	frame.RefID = "foo"

	var values2 = []*float64{f}
	frame2 := data.NewFrame("foo", data.NewField("value2", nil, values2))
	frame2.RefID = "foo"

	frames := []*data.Frame{frame, frame2}

	model := &data.Frame{}
	_, err := db.QueryFramesInto("foo", "select * from foo", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 2, model.Rows())
	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)
}

func TestTimestamps(t *testing.T) {
	db := NewInMemoryDB()

	var values = []time.Time{time.Now()}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"

	frames := []*data.Frame{frame}

	model := &data.Frame{}
	_, err := db.QueryFramesInto("foo", "select * from foo", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 1, model.Rows())
	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)
	assert.Contains(t, txt, "Type: []*time.Time")
}
