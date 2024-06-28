package duck

import (
	"fmt"
	"sync"
	"time"

	sdk "github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/scottlepp/go-duck/duck/data"
)

type FrameData struct {
	cacheDuration int
	cache         *cache
	db            *DuckDB
}

func (f *FrameData) Query(name string, query string, frames []*sdk.Frame) (string, bool, error) {
	dirs, cached, err := f.data(name, query, frames)
	if err != nil {
		logger.Error("error converting to parquet", "error", err)
		return "", cached, err
	}

	defer f.postProcess(name, query, dirs, cached)

	// create a wait group to wait for the query to finish
	// if the cache duration is exceeded, wait before deleting the cache ( parquet files )
	var wg sync.WaitGroup
	wg.Add(1)
	f.cache.setWait(fmt.Sprintf("%s:%s", name, query), &wg)

	var res string
	var qerr error

	go func() {
		res, qerr = f.runQuery(query, dirs, frames)
		wg.Done()
	}()

	wg.Wait()
	f.cache.deleteWait(fmt.Sprintf("%s:%s", name, query))

	if qerr != nil {
		logger.Error("error running commands", "error", err)
		return "", cached, err
	}

	key := fmt.Sprintf("%s:%s", name, query)
	if f.cacheDuration > 0 && !cached {
		f.cache.set(key, dirs)
	}

	return res, cached, nil
}

func (f *FrameData) runQuery(query string, dirs Dirs, frames []*sdk.Frame) (string, error) {
	commands := createViews(frames, dirs)
	commands = append(commands, query)
	return f.db.RunCommands(commands)
}

func createViews(frames []*sdk.Frame, dirs Dirs) []string {
	commands := []string{}
	created := map[string]bool{}
	logger.Debug("starting to create views from frames", "frames", len(frames))
	for _, frame := range frames {
		if created[frame.RefID] {
			continue
		}
		cmd := fmt.Sprintf("CREATE VIEW %s AS (SELECT * from '%s/*.parquet');", frame.RefID, dirs[frame.RefID])
		logger.Debug("creating view", "cmd", cmd)
		commands = append(commands, cmd)
		created[frame.RefID] = true
	}
	return commands
}

func (f *FrameData) data(name string, query string, frames []*sdk.Frame) (Dirs, bool, error) {
	if f.cacheDuration > 0 {
		// check the cache
		key := fmt.Sprintf("%s:%s", name, query)
		if d, ok := f.cache.get(key); ok {
			return d, true, nil
		}
	}

	dirs, err := data.ToParquet(frames, f.db.chunk)
	return dirs, false, err
}

func (f *FrameData) postProcess(name string, query string, dirs Dirs, cached bool) {
	go func() {
		if f.cacheDuration == 0 {
			wipe(dirs)
			return
		}
		// if result is not cached, a new cache entry will be created
		// delete the new cache entry after cacheDuration
		if !cached {
			time.Sleep(time.Duration(f.cacheDuration) * time.Second)
			key := fmt.Sprintf("%s:%s", name, query)
			f.cache.delete(key)
			// if the query is running wait for the query to finish before deleting the parquet files
			wg, wait := f.cache.getWait(key)
			if wait {
				wg.Wait()
				wipe(dirs)
				return
			}
			wipe(dirs)
		}
	}()
}

type cache struct {
	store sync.Map
	wait  sync.Map
}

func (c *cache) set(key string, value Dirs) {
	c.store.Store(key, value)
}

func (c *cache) get(key string) (Dirs, bool) {
	val, ok := c.store.Load(key)
	if !ok {
		return nil, false
	}
	return val.(Dirs), true
}

func (c *cache) delete(key string) {
	c.store.Delete(key)
}

func (c *cache) setWait(key string, value *sync.WaitGroup) {
	c.wait.Store(key, value)
}

func (c *cache) getWait(key string) (*sync.WaitGroup, bool) {
	val, ok := c.wait.Load(key)
	if !ok {
		return nil, false
	}
	return val.(*sync.WaitGroup), true
}

func (c *cache) deleteWait(key string) {
	c.wait.Delete(key)
}
