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
	ldbopt "github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/sync/errgroup"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

func main() {
	var (
		tests = genTests()
		testflag     = flag.String("test", "", "tests to run ("+strings.Join(testnames(tests), ", ")+")")
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
		if err := runTest(tests, *logdirflag, dbdir, name, cfg); err != nil {
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

func runTest(tests map[string]Benchmarker, logdir, dbdir, name string, cfg bench.WriteConfig) error {
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

const ldbDefaultCacheSize = 8 * 1024 * 1024
const ldbDefaultMemTableSize = 4 * 1024 * 1024

func makeDefaultOptions() pebble.Options {
	defaultOptions := pebble.Options{
                // Pebble has a single combined cache area and the write
                // buffers are taken from this too. Assign all available
                // memory allowance for cache.
		// TODO check ldb default Cache Size
		// 2mb cache size by default
                Cache:        pebble.NewCache(int64(ldbDefaultCacheSize)),
		// same as ldb default max handles
                MaxOpenFiles: 1000,
                // The size of memory table(as well as the write buffer).
                // Note, there may have more than two memory tables in the system.
                // MemTableStopWritesThreshold can be configured to avoid the memory abuse.
		// TODO check ldb default MemTableSize
		// TODO check this value is proper
                MemTableSize: ldbDefaultMemTableSize,
                // The default compaction concurrency(1 thread),
                // Here use all available CPUs for faster compaction.
                MaxConcurrentCompactions: func() int { return 1 }, // TODO set to 1 for compatibility with ldb.  up this?
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
	return defaultOptions
}

func makeLevels(size int64, count int)  []pebble.LevelOptions {
	result := []pebble.LevelOptions{}

	for i := 0; i < count; i++ {
		l := &pebble.LevelOptions{}
		l = l.EnsureDefaults()
		l.TargetFileSize = size
		result = append(result, *l)
	}
	return result
}

func genTests() map[string]Benchmarker {
	var tests = map[string]Benchmarker{}
	tests["nobatch"] = seqWrite{Options: makeDefaultOptions()}

	tests["nobatch-nosync"] = seqWrite{Options: makeDefaultOptions(), NoSyncOnWrite: true}

	tests["batch-100kb"] = batchWrite{Options: makeDefaultOptions(), BatchSize: 100 * ldbopt.KiB}
	tests["batch-1mb"] = batchWrite{Options: makeDefaultOptions(), BatchSize: ldbopt.MiB}
	tests["batch-5mb"] = batchWrite{Options: makeDefaultOptions(), BatchSize: 5 * ldbopt.MiB}

	batch_100kb_wb_512mb_cach_1gb := batchWrite{BatchSize: 100 * ldbopt.KiB, Options: makeDefaultOptions()}
	batch_100kb_wb_512mb_cach_1gb.Options.Cache = pebble.NewCache(1024 * ldbopt.MiB)
	batch_100kb_wb_512mb_cach_1gb.Options.MemTableSize = 512 * ldbopt.MiB
	tests["batch-100kb-wb-512mb-cache-1gb"] = batch_100kb_wb_512mb_cach_1gb

	tests["batch-100kb-nosync"] = batchWrite{BatchSize: 100 * 1024, Options: makeDefaultOptions(), NoSyncOnWrite: true}

	batch_100kb_wb_512mb_cache_1gb_nosync := batchWrite{BatchSize: 100 * 1024, Options: makeDefaultOptions(), NoSyncOnWrite: true}
	batch_100kb_wb_512mb_cache_1gb_nosync.Options.Cache = pebble.NewCache(1024 * ldbopt.MiB)
	batch_100kb_wb_512mb_cache_1gb_nosync.Options.MemTableSize = 512 * ldbopt.MiB
	tests["batch-100kb-wb-512mb-cache-1gb-nosync"] = batch_100kb_wb_512mb_cache_1gb_nosync

	batch_100kb_ctable_64mb := batchWrite{BatchSize: 100 * 1024, Options: makeDefaultOptions()}
	batch_100kb_ctable_64mb.Options.Levels = makeLevels(64 * 1024 * 1024, 1)
	tests["batch-100kb-ctable-64mb"] = batch_100kb_ctable_64mb

	batch_100kb_ctable_64mb_nosync := batchWrite{BatchSize: 100 * 1024, Options: makeDefaultOptions(), NoSyncOnWrite: true}
	batch_100kb_ctable_64mb_nosync.Options.Levels = makeLevels(64 * 1024 * 1024, 1)
	tests["batch-100kb-ctable-64mb-nosync"] = batch_100kb_ctable_64mb_nosync

	batch_100kb_ctable_64mb_wb_512mb_cache_1gb := batchWrite{BatchSize: 100 * 1024, Options: makeDefaultOptions()}
	batch_100kb_ctable_64mb_wb_512mb_cache_1gb.Options.Cache = pebble.NewCache(1024 * ldbopt.MiB)
	batch_100kb_ctable_64mb_wb_512mb_cache_1gb.Options.Levels = makeLevels(64 * 1024 * 1024, 1)
	batch_100kb_ctable_64mb_wb_512mb_cache_1gb.Options.MemTableSize = 512 * ldbopt.MiB
	tests["batch-100kb-ctable-64mb-wb-512mb-cache-1gb"] = batch_100kb_ctable_64mb_wb_512mb_cache_1gb

	// pebble doesn't support transactions. these aren't different than batch-XXX above. remove these?
	tests["batch-100kb-notx"] = batchWrite{Options: makeDefaultOptions(), BatchSize: 100 * ldbopt.KiB}
	tests["batch-1mb-notx"] = batchWrite{Options: makeDefaultOptions(), BatchSize: ldbopt.MiB}
	tests["batch-5mb-notx"] = batchWrite{Options: makeDefaultOptions(), BatchSize: 5 * ldbopt.MiB}

	concurrentOptions := concurrentWrite{N: 8, Options: makeDefaultOptions(), NoSyncOnWrite: true}

	tests["concurrent"] = concurrentOptions //concurrentWrite{N: 8, Options: makeDefaultOptions()}

	// TODO does pebble implement write batch merging (does it allow it to be disabled?)
	// TODO what is a use-case of NoWriteMerge in ldb? Unique sequence number per non-batch write?
	// tests["concurrent-nomerge"] = concurrentWrite{N: 8, NoWriteMerge: true}

	// for now just put incorrect test
	tests["concurrent-nomerge"] = concurrentOptions //concurrentWrite{N: 8, Options: makeDefaultOptions()}

	return tests
}

func testnames(tests map[string]Benchmarker) (n []string) {
	for name := range tests {
		n = append(n, name)
	}
	sort.Strings(n)
	return n
}

type seqWrite struct {
	Options pebble.Options
	NoSyncOnWrite bool
}

func (b seqWrite) Benchmark(dir string, env *bench.WriteEnv) error {
	db, err := pebble.Open(dir, &b.Options)
	if err != nil {
		return err
	}
	defer db.Close()
	wo := pebble.WriteOptions{Sync: !b.NoSyncOnWrite}

	return env.Run(func(key, value string, lastCall bool) error {
		if err := db.Set([]byte(key), []byte(value), &wo); err != nil {
			return err
		}
		env.Progress(len(value))
		return nil
	})
}

type batchWrite struct {
	Options   pebble.Options
	BatchSize int
	NoSyncOnWrite bool
}

func (b batchWrite) Benchmark(dir string, env *bench.WriteEnv) error {
	db, err := pebble.Open(dir, &b.Options)
	if err != nil {
		return err
	}
	defer db.Close()

	batch := db.NewBatch()
	bsize := 0
	wo := pebble.WriteOptions{Sync: !b.NoSyncOnWrite}
	return env.Run(func(key, value string, lastCall bool) error {
		batch.Set([]byte(key), []byte(value), nil)
		bsize += len(value)
		if bsize >= b.BatchSize || lastCall {
			if err := batch.Apply(batch, &wo); err != nil {
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
	Options      pebble.Options
	N            int
	NoWriteMerge bool
	NoSyncOnWrite bool
}

func (b concurrentWrite) Benchmark(dir string, env *bench.WriteEnv) error {
	db, err := pebble.Open(dir, &b.Options)
	if err != nil {
		return err
	}
	defer db.Close()

	var (
		write            = make(chan kv, b.N)
		outerCtx, cancel = context.WithCancel(context.Background())
		eg, ctx          = errgroup.WithContext(outerCtx)
		writeOpts = pebble.WriteOptions{Sync: !b.NoSyncOnWrite}
	)
	for i := 0; i < b.N; i++ {
		eg.Go(func() error {
			for {
				select {
				case kv := <-write:
					if err := db.Set([]byte(kv.k), []byte(kv.v), &writeOpts); err != nil {
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
