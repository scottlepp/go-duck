package duck

import (
	"fmt"
	"testing"
	"time"

	"github.com/araddon/dateparse"
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

	res, _, err := db.QueryFrames("foo", "select * from foo", frames)
	assert.Nil(t, err)

	assert.Contains(t, res, `[{"value":"test"}]`)
}

func TestQueryFrameCache(t *testing.T) {
	opts := Opts{
		CacheDuration: 5,
	}
	db := NewInMemoryDB(opts)

	var values = []string{"test"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"
	frames := []*data.Frame{frame}

	res, cached, err := db.QueryFrames("foo", "select * from foo", frames)
	assert.Nil(t, err)
	assert.False(t, cached)
	assert.Contains(t, res, `[{"value":"test"}]`)

	res, cached, err = db.QueryFrames("foo", "select * from foo", frames)
	assert.Nil(t, err)
	assert.True(t, cached)
	assert.Contains(t, res, `[{"value":"test"}]`)

	// wait for cache to expire
	time.Sleep(6 * time.Second)

	res, cached, err = db.QueryFrames("foo", "select * from foo", frames)
	assert.Nil(t, err)
	assert.False(t, cached)
	assert.Contains(t, res, `[{"value":"test"}]`)
}

func TestQueryFrameWithDisplayName(t *testing.T) {

	db := NewInMemoryDB()

	var values = []string{"test"}
	field := data.NewField("value", nil, values)
	field.Config = &data.FieldConfig{
		DisplayName: "some value",
	}
	frame := data.NewFrame("foo", field)
	frame.RefID = "foo"
	frames := []*data.Frame{frame}

	res, _, err := db.QueryFrames("foo", "select * from foo", frames)
	assert.Nil(t, err)

	assert.Contains(t, res, `[{"some value":"test"}]`)
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

	res, _, err := db.QueryFrames("foo", "select * from foo", frames)
	assert.Nil(t, err)

	assert.Contains(t, res, `test2`)
}

func TestQueryFrameIntoFrame(t *testing.T) {
	db := NewInMemoryDB()

	var values = []string{"2024-02-23 09:01:54"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"

	var values2 = []string{"2024-02-23 09:02:54"}
	frame2 := data.NewFrame("foo", data.NewField("value", nil, values2))
	frame2.RefID = "foo"

	frames := []*data.Frame{frame, frame2}

	model := &data.Frame{}
	err := db.QueryFramesInto("foo", "select * from foo order by value desc", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 2, model.Rows())

	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)
}

func TestQueryFrameIntoFrameMultipleColumns(t *testing.T) {
	db := NewInMemoryDB()

	frame := data.NewFrame(
		"A",
		data.NewField("Z State", nil, []string{"Alaska"}),
		data.NewField("Y Lat", nil, []string{"61"}),
		data.NewField("X Lng", nil, []string{"32"}),
	)
	frame.RefID = "A"

	frames := []*data.Frame{frame}

	model := &data.Frame{}
	err := db.QueryFramesInto("B", "select * from A", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, "Z State", model.Fields[0].Name)
	assert.Equal(t, "Y Lat", model.Fields[1].Name)
	assert.Equal(t, "X Lng", model.Fields[2].Name)

	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)
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
	err := db.QueryFramesInto("foo", "select * from foo", frames, model)
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
	err := db.QueryFramesInto("foo", "select * from foo", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 2, model.Rows())
	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)
}

func TestTimestamps(t *testing.T) {
	db := NewInMemoryDB()

	tt := "2024-02-23 09:01:54"
	dd, err := dateparse.ParseAny(tt)
	assert.Nil(t, err)

	var values = []time.Time{dd}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"

	frames := []*data.Frame{frame}

	model := &data.Frame{}
	err = db.QueryFramesInto("foo", "select * from foo", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 1, model.Rows())
	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)
	assert.Contains(t, txt, "Type: []*time.Time")
}

func TestLabels(t *testing.T) {
	db := NewInMemoryDB()

	f := new(float64)
	*f = 12345

	var values = []*float64{f}
	labels := map[string]string{
		"server": "A",
	}
	frame := data.NewFrame("foo", data.NewField("value1", labels, values))
	frame.RefID = "foo"

	var values2 = []*float64{f}
	labels2 := map[string]string{
		"server": "B",
	}
	frame2 := data.NewFrame("foo", data.NewField("value2", labels2, values2))
	frame2.RefID = "foo"

	frames := []*data.Frame{frame, frame2}

	model := &data.Frame{}
	err := db.QueryFramesInto("foo", "select * from foo", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 2, model.Rows())
	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)

	assert.Contains(t, txt, "server")
	assert.Contains(t, txt, "A")
	assert.Contains(t, txt, "B")
}

// TODO - neeed to return 2 frames here
// or just append the labels to the fields???
func TestLabelsMultiFrame(t *testing.T) {
	db := NewInMemoryDB()

	tt := "2024-02-23 09:01:54"
	dd, err := dateparse.ParseAny(tt)
	assert.Nil(t, err)

	ttt := "2024-02-23 09:02:54"
	ddd, err := dateparse.ParseAny(ttt)
	assert.Nil(t, err)

	var timeValues = []time.Time{dd, ddd}

	f := new(float64)
	*f = 12345

	var values = []*float64{f, f}
	labels := map[string]string{
		"server": "A",
	}
	frame := data.NewFrame("foo", data.NewField("timestamp", nil, timeValues), data.NewField("value", labels, values))
	frame.RefID = "foo"

	var values2 = []*float64{f, f}
	labels2 := map[string]string{
		"server": "B",
	}
	frame2 := data.NewFrame("foo", data.NewField("timestamp", nil, timeValues), data.NewField("value", labels2, values2))
	frame2.RefID = "foo"

	frames := []*data.Frame{frame, frame2}

	// TODO - ordering is broken!
	model := &data.Frame{}
	err = db.QueryFramesInto("foo", "select * from foo order by timestamp desc", frames, model)
	assert.Nil(t, err)

	assert.Equal(t, 4, model.Rows())
	txt, err := model.StringTable(-1, -1)
	assert.Nil(t, err)

	fmt.Printf("GOT: %s", txt)

	assert.Contains(t, txt, "server")
	assert.Contains(t, txt, "A")
	assert.Contains(t, txt, "B")
}

// TODO - don't think this is valid to have a frame with duplicate fields
// func TestWideFrameWithDuplicateFields(t *testing.T) {
// 	db := NewInMemoryDB()

// 	tt := "2024-02-23 09:01:54"
// 	dd, err := dateparse.ParseAny(tt)
// 	assert.Nil(t, err)

// 	ttt := "2024-02-23 09:02:54"
// 	ddd, err := dateparse.ParseAny(ttt)
// 	assert.Nil(t, err)

// 	var timeValues = []time.Time{dd, ddd}

// 	f := new(float64)
// 	*f = 12345

// 	var values = []*float64{f, f}
// 	labels := map[string]string{
// 		"server": "A",
// 	}

// 	var values2 = []*float64{f, f}
// 	labels2 := map[string]string{
// 		"server": "B",
// 	}
// 	frame := data.NewFrame("foo",
// 		data.NewField("timestamp", nil, timeValues),
// 		data.NewField("value", labels, values),
// 		data.NewField("value", labels2, values2),
// 	)

// 	frame.RefID = "foo"

// 	frames := []*data.Frame{frame}

// 	// TODO - ordering is broken!
// 	model := &data.Frame{}
// 	err = db.QueryFramesInto("foo", "select * from foo order by timestamp desc", frames, model)
// 	assert.Nil(t, err)

// 	assert.Equal(t, 2, model.Rows())
// 	txt, err := model.StringTable(-1, -1)
// 	assert.Nil(t, err)

// 	fmt.Printf("GOT: %s", txt)

// 	assert.Contains(t, txt, "server")
// 	assert.Contains(t, txt, "A")
// 	assert.Contains(t, txt, "B")
// }
