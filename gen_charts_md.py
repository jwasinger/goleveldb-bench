import glob

def order_benchmarks(benchmark_files: [str]) -> [str]:
    result = sorted(set([(fname.split("/")[-1]).replace("-pebble.svg", "").replace("-ldb.svg", "") for fname in benchmark_files]))
    final_result = []
    for item in result:
        final_result.append(item+"-pebble.svg")
        final_result.append(item+"-ldb.svg")

    return final_result

i = 0
print("## write benchmarks")
for fname in order_benchmarks(glob.glob("write-charts/*")):
    print("#![](https://github.com/jwasinger/goleveldb-bench/raw/pebble/write-charts/{})".format(fname.split("/")[-1].strip('\n')))
    if i % 2 != 0:
        print()
        print("---")
        print()
    i += 1

i = 0
print("## read benchmarks")
for fname in order_benchmarks(glob.glob("read-charts/*")):
    print("#![](https://github.com/jwasinger/goleveldb-bench/raw/pebble/read-charts/{})".format(fname.split("/")[-1].strip('\n')))
    if i % 2 != 0:
        print()
        print("---")
        print()
    i += 1
