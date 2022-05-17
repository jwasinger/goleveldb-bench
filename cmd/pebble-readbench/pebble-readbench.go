package main

import (
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	bench "github.com/fjl/goleveldb-bench"
	ldbopt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

func main() {
	var (
		testflag     = flag.String("test", "", "tests to run ("+strings.Join(testnames(), ", ")+")")
		sizeflag     = flag.String("size", "500mb", "total amount of value data to write")
		datasizeflag = flag.String("valuesize", "100b", "size of each value")
		keysizeflag  = flag.String("keysize", "32b", "size of each key")
		dirflag      = flag.String("dir", ".", "test database directory")
		logdirflag   = flag.String("logdir", ".", "test log output directory")
		deletedbflag = flag.Bool("deletedb", false, "delete databases after test run")

		run []string
		cfg bench.ReadConfig
		err error
	)
	flag.Parse()

	for _, t := range strings.Split(*testflag, ",") {
		if tests[t] == nil {
			log.Fatalf("unknown test %q", t)
		}
		run = append(run, t)
	}
	if len(run) == 0 {
		log.Fatal("no tests to run, use -test to select tests")
	}
	if cfg.Size, err = bench.ParseSize(*sizeflag); err != nil {
		log.Fatal("-size: ", err)
	}
	if cfg.DataSize, err = bench.ParseSize(*datasizeflag); err != nil {
		log.Fatal("-datasize: ", err)
	}
	if cfg.KeySize, err = bench.ParseSize(*keysizeflag); err != nil {
		log.Fatal("-datasize: ", err)
	}
	cfg.LogPercent = true

	if err := os.MkdirAll(*logdirflag, 0755); err != nil {
		log.Fatal("can't create log dir: %v", err)
	}

	anyErr := false
	for _, name := range run {
		var (
			dbdir    string
			createdb bool
		)
		// The given dir points to an existent directory, assume it's
		// a old database for read testing.
		if isDir(*dirflag) && fileExist(filepath.Join(*dirflag, "testing.key")) {
			if strings.Contains(*dirflag, "filter") != strings.Contains(name, "filter") {
				log.Printf("Skip test %s. Incompatible database", name)
				continue
			}
			dbdir = *dirflag
		} else {
			dbdir, createdb = filepath.Join(*dirflag, "testdb-"+name), true
		}
		if err := os.MkdirAll(dbdir, 0755); err != nil {
			log.Fatal("can't create keyfile dir: %v", err)
		}
		if err := runTest(*logdirflag, dbdir, name, createdb, cfg); err != nil {
			log.Printf("test %q failed: %v", name, err)
			anyErr = true
		}
		if *deletedbflag {
			os.RemoveAll(dbdir)
		}
	}
	if anyErr {
		log.Fatal("one ore more tests failed")
	}
}

func runTest(logdir, dbdir, name string, createdb bool, cfg bench.ReadConfig) error {
	cfg.TestName = name
	logfile, err := os.Create(filepath.Join(logdir, name+time.Now().Format(".2006-01-02-15:04:05")+".json"))
	if err != nil {
		return err
	}
	defer logfile.Close()

	var (
		kw    io.Writer
		kr    io.Reader
		reset func()
		kfile = filepath.Join(dbdir, "testing.key")
	)
	if !createdb {
		keyfile, err := os.Open(kfile)
		if err != nil {
			return err
		}
		defer keyfile.Close()
		kr = keyfile
	} else {
		keyfile, err := os.Create(kfile)
		if err != nil {
			return err
		}
		defer keyfile.Close()
		kw, kr = keyfile, keyfile
		reset = func() {
			keyfile.Seek(0, io.SeekStart)
		}
	}

	log.Printf("== running %q", name)
	env := bench.NewReadEnv(logfile, kr, kw, reset, cfg)
	return tests[name].Benchmark(dbdir, env)
}

type Benchmarker interface {
	Benchmark(dir string, env *bench.ReadEnv) error
}

func makeDefaultOptions(blockCacheSize int64, filterPolicy bloom.FilterPolicy) pebble.Options {
        defaultOptions := pebble.Options{
                // Pebble has a single combined cache area and the write
                // buffers are taken from this too. Assign all available
                // memory allowance for cache.
                // TODO check ldb default Cache Size
		// TODO verify this is the same as ldb.Options.BlockCacheCapacity
                // 2mb cache size by default
                Cache:        pebble.NewCache(blockCacheSize),
                // TODO check equivalent default setting in ldb for this
                // TODO why tf is this 0 by default in Geth???
                MaxOpenFiles: 0,
                // The size of memory table(as well as the write buffer).
                // Note, there may have more than two memory tables in the system.
                // MemTableStopWritesThreshold can be configured to avoid the memory abuse.
                // TODO check ldb default MemTableSize
                // TODO check this value is proper
                MemTableSize: 2 * 1024 * 1024 / 4,
                // The default compaction concurrency(1 thread),
                // Here use all available CPUs for faster compaction.
                MaxConcurrentCompactions: 1, // TODO set to 1 for compatibility with ldb.  up this?
                // Per-level options. Options for at least one level must be specified. The
                // options for the last level are used for all subsequent levels.
                // TODO each level TargetFileSize should be 10x larger than the parent
                Levels: []pebble.LevelOptions{
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: filterPolicy},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: filterPolicy},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: filterPolicy},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: filterPolicy},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: filterPolicy},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: filterPolicy},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: filterPolicy},
                },
        }
        return defaultOptions
}

var tests = map[string]Benchmarker{
	"random-read": randomRead{Options: makeDefaultOptions(8 * ldbopt.MiB, bloom.FilterPolicy(10))},
	"random-read-filter": randomRead{Options: makeDefaultOptions(8 * ldbopt.MiB, bloom.FilterPolicy(0))}, // TODO check that FilterPolicy(0) is correct
	"random-read-bigcache": randomRead{Options: makeDefaultOptions(100 * ldbopt.MiB, bloom.FilterPolicy(0))},
	"random-read-bigcache-filter": randomRead{Options: makeDefaultOptions(100 * ldbopt.MiB, bloom.FilterPolicy(10))},
}

func testnames() (n []string) {
	for name := range tests {
		n = append(n, name)
	}
	sort.Strings(n)
	return n
}

type randomRead struct {
	Options pebble.Options
}

func (b randomRead) Benchmark(dir string, env *bench.ReadEnv) error {
	db, err := pebble.Open(dir, &b.Options)
	if err != nil {
		return err
	}
	defer db.Close()
	return env.Run(func(key, value string, lastCall bool) error {
		if err := db.Set([]byte(key), []byte(value), nil); err != nil {
			return err
		}
		return nil
	}, func(key string) error {
		if value, closer, err := db.Get([]byte(key)); err != nil {
			return err
		} else {
			valLen := len(value)
			closer.Close()
			env.Progress(valLen)
		}
		return nil
	})
}

func fileExist(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func isDir(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		return false
	}
	return f.Mode().IsDir()
}
