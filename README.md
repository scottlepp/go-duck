# Go wrapper for [DuckDB CLI](https://duckdb.org/docs/api/cli/overview)
* Doesn't require CGO.
* Requires duckdb cli to be in the path.

## Dataframes
* Allows querying dataframes using SQL, with all the aggregate/window/analytics functions from DuckDB.

## In Memory Database
```
	db := NewInMemoryDB()

	commands := []string{
		"CREATE TABLE t1 (i INTEGER, j INTEGER);",
		"INSERT INTO t1 VALUES (1, 5);",
		"SELECT * from t1;",
	}
	res, err := db.RunCommands(commands)
```
## File based Database
```
	db := NewDuckDB("foo")

	commands := []string{
		"CREATE TABLE t1 (i INTEGER, j INTEGER);",
		"INSERT INTO t1 VALUES (1, 5);",
	}
	_, err := db.RunCommands(commands)
	res, err := db.Query("SELECT * from t1;")
```
## Query Dataframes
```
	db := NewInMemoryDB()

	var values = []string{"test"}
	frame := data.NewFrame("foo", data.NewField("value", nil, values))
	frame.RefID = "foo"
	frames := []*data.Frame{frame}

	res, err := db.QueryFrames("foo", "select * from foo", frames)
```
