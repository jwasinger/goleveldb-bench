package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	bench "github.com/fjl/goleveldb-bench"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt" ldbopt
	"golang.org/x/sync/errgroup"
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
		cfg bench.WriteConfig
		err error
	)
	flag.Parse()

	for _, t := range strings.Split(*testflag, ",") {
		t = strings.TrimSpace(t)
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
		dbdir := filepath.Join(*dirflag, "testdb-"+name)
		if err := runTest(*logdirflag, dbdir, name, cfg); err != nil {
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

func runTest(logdir, dbdir, name string, cfg bench.WriteConfig) error {
	cfg.TestName = name
	logfile, err := os.Create(filepath.Join(logdir, name+".json"))
	if err != nil {
		return err
	}
	defer logfile.Close()
	log.Printf("== running %q", name)
	env := bench.NewWriteEnv(logfile, cfg)
	return tests[name].Benchmark(dbdir, env)
}

type Benchmarker interface {
	Benchmark(dir string, env *bench.WriteEnv) error
}

func defaultPebbleOptions() *pebble.Options {
	defaultOptions := &pebble.Options{
                // Pebble has a single combined cache area and the write
                // buffers are taken from this too. Assign all available
                // memory allowance for cache.
		// TODO check ldb default Cache Size
                Cache:        pebble.NewCache(int64(cache * 1024 * 1024)),
		// TODO check equivalent default setting in ldb for this
                MaxOpenFiles: handles,
                // The size of memory table(as well as the write buffer).
                // Note, there may have more than two memory tables in the system.
                // MemTableStopWritesThreshold can be configured to avoid the memory abuse.
		// TODO check ldb default MemTableSize
                MemTableSize: cache * 1024 * 1024 / 4,
                // The default compaction concurrency(1 thread),
                // Here use all available CPUs for faster compaction.
                MaxConcurrentCompactions: 1, // TODO set to 1 for compatibility with ldb.  up this?
                // Per-level options. Options for at least one level must be specified. The
                // options for the last level are used for all subsequent levels.
		// TODO each level TargetFileSize should be 10x larger than the parent
                Levels: []pebble.LevelOptions{
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
                        {TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
                },
        }
}

func genTests() map[string]Benchmarker {
	var tests = map[string]Benchmarker{}
	tests["nobatch"] = seqWrite{}

	tests["nobatch-nosync"] = seqWrite{Options: makeDefaultOptions()}
	tests["nobatch-nosync"].Options.WriteOptions.Sync = false

	tests["batch-100kb"] = batchWrite{Options: makeDefaultOptions(), BatchSize: 100 * ldbopt.KiB}
	tests["batch-1mb"] = batchWrite{Options: makeDefaultOptions(), BatchSize: ldbopt.MiB}
	tests["batch-5mb"] = batchWrite{Options: makeDefaultOptions(), BatchSize: 5 * ldbopt.MiB}

	tests["batch-100kb-wb-512mb-cache-1gb"] = batchWrite{BatchSize: 100 * ldbopt.KiB, Options: makeDefaultOptions()}
	tests["batch-100kb-wb-512mb-cache-1gb"].Options.Cache = pebble.NewCache(1024 * ldbopt.MiB)
	tests["batch-100kb-wb-512mb-cache-1gb"].Options.MemtableSize = 512 * ldbopt.MiB

	tests["batch-100kb-nosync"] = batchWrite{BatchSize: 100 * 1024, Options: makeDefaultOptions()}
	tests["batch-100kb-nosync"].Options.WriteOptions.Sync = false

	tests["batch-100kb-wb-512mb-cache-1gb-nosync"] = batchWrite{BatchSize = 100 * 1024, Options: makeDefaultOptions()}
	tests["batch-100kb-wb-512mb-cache-1gb-nosync"].Options.WriteOptions.Sync = false
	tests["batch-100kb-wb-512mb-cache-1gb-nosync"].Options.Cache = pebble.NewCache(1024 * ldbopt.MiB)
	tests["batch-100kb-wb-512mb-cache-1gb-nosync"].Options.MemtableSize = 512 * ldbopt.MiB

	tests["batch-100kb-ctable-64mb"] = batchWrite{BatchSize: 100 * 1024, Options: makeDefaultOptions()}
	tests["batch-100kb-ctable-64mb"] = 
}

var tests = map[string]Benchmarker{
	"nobatch":        seqWrite{},
	// NoSync (ldb) -> WriteOptions.Sync (pebble) (TODO verify this)
	"nobatch-nosync": seqWrite{Options: opt.Options{NoSync: true}},
	"batch-100kb":    batchWrite{BatchSize: 100 * opt.KiB},
	"batch-1mb":      batchWrite{BatchSize: opt.MiB},
	"batch-5mb":      batchWrite{BatchSize: 5 * opt.MiB},
	"batch-100kb-wb-512mb-cache-1gb": batchWrite{
		BatchSize: 100 * 1024,
		Options: opt.Options{
			// These settings approximate what geth is doing.
			// Cache: pebble.NewCache(size in bytes)
			BlockCacheCapacity: 1024 * opt.MiB,
			// Options.MemtableSize
			WriteBuffer:        512 * opt.MiB,
		},
	},
	"batch-100kb-nosync": batchWrite{
		BatchSize: 100 * 1024,
		Options:   opt.Options{NoSync: true},
	},
	"batch-100kb-wb-512mb-cache-1gb-nosync": batchWrite{
		BatchSize: 100 * 1024,
		Options: opt.Options{
			NoSync:             true,
			BlockCacheCapacity: 1024 * opt.MiB,
			WriteBuffer:        512 * opt.MiB,
		},
	},
	"batch-100kb-ctable-64mb": batchWrite{
		BatchSize: 100 * 1024,
		// CompactionTableSize (ldb) -> 
		Options:   opt.Options{CompactionTableSize: 64 * opt.MiB},
	},
	"batch-100kb-ctable-64mb-nosync": batchWrite{
		BatchSize: 100 * 1024,
		Options:   opt.Options{NoSync: true, CompactionTableSize: 64 * opt.MiB},
	},
	"batch-100kb-ctable-64mb-wb-512mb-cache-1gb": batchWrite{
		BatchSize: 100 * 1024,
		Options: opt.Options{
			BlockCacheCapacity:  1024 * opt.MiB,
			WriteBuffer:         512 * opt.MiB,
			CompactionTableSize: 64 * opt.MiB,
		},
	},
	"batch-100kb-notx": batchWrite{
		BatchSize: 1024 * 1024,
		// Pebble doesn't support transactions
		Options:   opt.Options{DisableLargeBatchTransaction: true},
	},
	"batch-1mb-notx": batchWrite{
		BatchSize: 1024 * 1024,
		Options:   opt.Options{DisableLargeBatchTransaction: true},
	},
	"batch-5mb-notx": batchWrite{
		BatchSize: 5 * 1024 * 1024,
		Options:   opt.Options{DisableLargeBatchTransaction: true},
	},
	"concurrent":         concurrentWrite{N: 8},
	"concurrent-nomerge": concurrentWrite{N: 8, NoWriteMerge: true},
}

func testnames() (n []string) {
	for name := range tests {
		n = append(n, name)
	}
	sort.Strings(n)
	return n
}

type seqWrite struct {
	Options opt.Options
}

func (b seqWrite) Benchmark(dir string, env *bench.WriteEnv) error {
	db, err := leveldb.OpenFile(dir, &b.Options)
	if err != nil {
		return err
	}
	defer db.Close()
	return env.Run(func(key, value string, lastCall bool) error {
		if err := db.Put([]byte(key), []byte(value), nil); err != nil {
			return err
		}
		env.Progress(len(value))
		return nil
	})
}

type batchWrite struct {
	Options   opt.Options
	BatchSize int
}

func (b batchWrite) Benchmark(dir string, env *bench.WriteEnv) error {
	db, err := leveldb.OpenFile(dir, &b.Options)
	if err != nil {
		return err
	}
	defer db.Close()

	batch := new(leveldb.Batch)
	bsize := 0
	return env.Run(func(key, value string, lastCall bool) error {
		batch.Put([]byte(key), []byte(value))
		bsize += len(value)
		if bsize >= b.BatchSize || lastCall {
			if err := db.Write(batch, nil); err != nil {
				return err
			}
			env.Progress(bsize)
			bsize = 0
			batch.Reset()
		}
		return nil
	})
}

type kv struct{ k, v string }

type concurrentWrite struct {
	Options      opt.Options
	N            int
	NoWriteMerge bool
}

func (b concurrentWrite) Benchmark(dir string, env *bench.WriteEnv) error {
	db, err := leveldb.OpenFile(dir, &b.Options)
	if err != nil {
		return err
	}
	defer db.Close()

	var (
		write            = make(chan kv, b.N)
		wopt             = &opt.WriteOptions{NoWriteMerge: b.NoWriteMerge}
		outerCtx, cancel = context.WithCancel(context.Background())
		eg, ctx          = errgroup.WithContext(outerCtx)
	)
	for i := 0; i < b.N; i++ {
		eg.Go(func() error {
			for {
				select {
				case kv := <-write:
					if err := db.Put([]byte(kv.k), []byte(kv.v), wopt); err != nil {
						return err
					}
					env.Progress(len(kv.v))
				case <-ctx.Done():
					return nil
				}
			}
		})
	}

	return env.Run(func(key, value string, lastCall bool) error {
		select {
		case write <- kv{k: key, v: value}:
		case <-ctx.Done():
			lastCall = true
		}
		if lastCall {
			cancel()
			return eg.Wait()
		}
		return nil
	})
}
