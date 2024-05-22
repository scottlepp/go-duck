package data

import (
	"bytes"
	"fmt"
	"os/exec"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func TestWrite(t *testing.T) {
	var values = []string{"test"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"
	frames := []*data.Frame{frame}

	dir, err := ToParquet(frames, 0)
	if err != nil {
		fmt.Println(err.Error())
		t.Fail()
	}
	fmt.Println(dir)
}

func TestRead(t *testing.T) {
	t.Skip() // need parquet file to test
	fmt.Println("test")
	var b bytes.Buffer
	b.Write([]byte(".mode json \n"))
	b.Write([]byte("SELECT * from foo.parquet; \n"))

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	cmd := exec.Command("duckdb", "")
	cmd.Stdin = &b
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run() // add error checking
	if err != nil {
		t.Fail()
	}

	fmt.Println(stdout.String())
	fmt.Println(stderr.String())
}
