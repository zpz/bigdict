# This code is adapted from Dobatymo/lmdb-python-dbm

import json
import os
import os.path
import pathlib
import pickle  # nosec
import shutil
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import closing, suppress
from random import randrange
from typing import Any, Callable, ContextManager, DefaultDict, Dict, Iterable, List, Sequence, TextIO

from genutility.iter import batch
from genutility.time import MeasureTime
from pytablewriter import MarkdownTableWriter

import lmdbm
import lmdbm.lmdbm
from bigdict import Bigdict


ResultsDict = Dict[int, Dict[str, Dict[str, float]]]

# Do not continue benchmark if the current
# step requires more seconds than MAX_TIME
MAX_TIME = 10
# BATCH_SIZE = 10000
BATCH_SIZE = 1000  # the speed gain is not very sensitive to this batch size as long as it's reasonably large



class BaseBenchmark(ABC):
    def __init__(self, db_tpl, db_type):
        self.available = True
        self.batch_write_available = True
        self.batch_read_available = False
        self.path = db_tpl.format(db_type)
        self.name = db_type
        self.write = -1
        self.batch_write = -1
        self.read = -1
        self.batch_read = -1
        self.combined = -1

    @abstractmethod
    def open(self) -> ContextManager:
        """Open the database"""

        pass

    def commit(self) -> None:  # noqa: B027
        """Commit the changes, if it is not done automatically"""

        pass

    def purge(self) -> None:
        """Remove the database file(s)"""

        with suppress(FileNotFoundError):
            os.unlink(self.path)

    def encode(self, value: Any) -> Any:
        """Convert Python objects to database-capable ones"""

        return value

    def decode(self, value: Any) -> Any:
        """Convert database values to Python objects"""

        return value

    def measure_writes(self, N: int) -> None:
        with MeasureTime() as t, self.open() as db:
            for key, value in self.generate_data(N):
                if t.get() > MAX_TIME:
                    break
                db[key] = self.encode(value)
                self.commit()
        if t.get() < MAX_TIME:
            self.write = t.get()
        self.print_time("write", N, t)

    def measure_batch_writes(self, N: int) -> None:
        with MeasureTime() as t, self.open() as db:
            for pairs in batch(self.generate_data(N), BATCH_SIZE):
                if t.get() > MAX_TIME:
                    break
                db.update({key: self.encode(value) for key, value in pairs})
                self.commit()
        if t.get() < MAX_TIME:
            self.batch_write = t.get()
        self.print_time("batch write", N, t)

    def measure_reads(self, N: int) -> None:
        with MeasureTime() as t, self.open() as db:
            for key in self.random_keys(N, N):
                if t.get() > MAX_TIME:
                    break
                self.decode(db[key])
        if t.get() < MAX_TIME:
            self.read = t.get()
        self.print_time("read", N, t)

    def measure_batch_reads(self, N: int) -> None:
        with MeasureTime() as t, self.open() as db:
            for keys in batch(self.random_keys(N, N), BATCH_SIZE):
                if t.get() > MAX_TIME:
                    break
                _ = [self.decode(v) for k, v in db.stream_get(keys)]
        if t.get() < MAX_TIME:
            self.batch_read = t.get()
        self.print_time("batch read", N, t)

    def measure_combined(self, read=1, write=10, repeat=100) -> None:
        with MeasureTime() as t, self.open() as db:
            for _ in range(repeat):
                if t.get() > MAX_TIME:
                    break
                for key, value in self.generate_data(read):  # ??
                    db[key] = self.encode(value)
                    self.commit()
                for key in self.random_keys(10, write):  # ??
                    self.decode(db[key])
        if t.get() < MAX_TIME:
            self.combined = t.get()
        self.print_time("combined", (read + write) * repeat, t)

    def database_is_built(self):
        return self.batch_write >= 0 or self.write >= 0

    def print_time(self, measure_type, numbers, t):
        print(f"{self.name:<20s} {measure_type:<15s} {str(numbers):<10s} {t.get():10.5f}")

    @staticmethod
    def generate_data(size):
        for i in range(size):
            yield "key_" + str(i), {"some": "object_" + str(i)}

    @staticmethod
    def random_keys(num, size):
        for _ in range(num):
            yield "key_" + str(randrange(0, size))  # nosec


class JsonEncodedBenchmark(BaseBenchmark):
    def encode(self, value):
        return json.dumps(value)

    def decode(self, value):
        return json.loads(value)


class DummyPickleBenchmark(BaseBenchmark):
    class MyDict(dict):
        def close(self):
            pass

    def __init__(self, db_tpl):
        super().__init__(db_tpl, "dummypickle")
        self.native_dict = None

    def open(self):
        if pathlib.Path(self.path).exists():
            with open(self.path, "rb") as f:
                self.native_dict = self.MyDict(pickle.load(f))  # nosec
        else:
            self.native_dict = self.MyDict()
        return closing(self.native_dict)

    def commit(self):
        tmp_file = self.path + ".tmp"
        with open(tmp_file, "wb") as f:
            pickle.dump(self.native_dict, f)
        shutil.move(tmp_file, self.path)


class DummyJsonBenchmark(BaseBenchmark):
    class MyDict(dict):
        def close(self):
            pass

    def __init__(self, db_tpl):
        super().__init__(db_tpl, "dummyjson")
        self.native_dict = None

    def open(self):
        if pathlib.Path(self.path).exists():
            with open(self.path) as f:
                self.native_dict = self.MyDict(json.load(f))
        else:
            self.native_dict = self.MyDict()
        return closing(self.native_dict)

    def commit(self):
        tmp_file = self.path + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(self.native_dict, f, ensure_ascii=False, check_circular=False, sort_keys=False)
        shutil.move(tmp_file, self.path)



class LdbmBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, "lmdbm")

    def open(self):
        return lmdbm.Lmdb.open(self.path, "c")

    def purge(self):
        lmdbm.lmdbm.remove_lmdbm(self.path)


class MyBigdict(Bigdict):
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

    # To be more comparable to LdbmBenchmark
    def encode_key(self, key):
        if isinstance(key, bytes):
            return key
        elif isinstance(key, str):
            return key.encode("Latin-1")

        raise TypeError(key)

    def decode_key(self, key):
        return key

    def encode_value(self, value):
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("Latin-1")

        raise TypeError(value)

    def decode_value(self, x):
        return x


class BigdictBenchmark(JsonEncodedBenchmark):
    def __init__(self, db_tpl):
        super().__init__(db_tpl, 'bigdict')
        self.batch_read_available = True

    def open(self):
        if os.path.isdir(self.path):
            return MyBigdict(self.path, readonly=False, map_size_mb=1000)
        else:
            return MyBigdict.new(self.path, map_size_mb=1000)
    
    def purge(self):
        try:
            MyBigdict(self.path, readonly=False).destroy()
        except FileNotFoundError:
            pass


BENCHMARK_CLASSES = [
    BigdictBenchmark,
    LdbmBenchmark,
    # DummyPickleBenchmark,
    # DummyJsonBenchmark,
]


def run_bench(N, db_tpl) -> Dict[str, Dict[str, float]]:
    benchmarks = [C(db_tpl) for C in BENCHMARK_CLASSES]

    for benchmark in benchmarks:
        if not benchmark.available:
            continue
        benchmark.purge()
        benchmark.measure_writes(N)
        if benchmark.batch_write_available:
            benchmark.purge()
            benchmark.measure_batch_writes(N)
        if benchmark.database_is_built():
            benchmark.measure_reads(N)
            if benchmark.batch_read_available:
                benchmark.measure_batch_reads(N)
            benchmark.measure_combined(read=1, write=10, repeat=100)

    ret: DefaultDict[str, Dict[str, float]] = defaultdict(dict)
    for benchmark in benchmarks:
        ret[benchmark.name]["read"] = benchmark.read
        ret[benchmark.name]["batch_read"] = benchmark.batch_read
        ret[benchmark.name]["write"] = benchmark.write
        ret[benchmark.name]["batch_write"] = benchmark.batch_write
        ret[benchmark.name]["combined"] = benchmark.combined

    return ret


def bench(base: str, nums: Iterable[int]) -> ResultsDict:
    with suppress(FileExistsError):
        os.mkdir(base)

    ret = {}
    db_tpl = os.path.join(base, "test_{}.db")

    for num in nums:
        print("")
        ret[num] = run_bench(num, db_tpl)

    return ret


def write_markdown_table(stream: TextIO, results: ResultsDict, method: str):
    for v in results.values():
        headers = list(v.keys())
        break

    value_matrix = []
    for k, v in results.items():
        row = [str(k)]
        for h in headers:
            value = v[h].get(method)
            if value is None or value < 0:
                new_value = "-"
            else:
                new_value = format(value, ".04f")
            row.append(new_value)
        value_matrix.append(row)

    headers = ["items"] + headers

    writer = MarkdownTableWriter(table_name=method, headers=headers, value_matrix=value_matrix)
    writer.dump(stream, close_after_write=False)


def _check_same_keys(dicts: Sequence[dict]):
    assert len(dicts) >= 2

    for d in dicts[1:]:
        assert dicts[0].keys() == d.keys()


def merge_results(results: Sequence[ResultsDict], func: Callable = min) -> ResultsDict:
    out: ResultsDict = {}

    _check_same_keys(results)
    for key1 in results[0].keys():
        _check_same_keys([d[key1] for d in results])
        out.setdefault(key1, {})
        for key2 in results[0][key1].keys():
            _check_same_keys([d[key1][key2] for d in results])
            out[key1].setdefault(key2, {})
            for key3 in results[0][key1][key2].keys():
                out[key1][key2][key3] = func(d[key1][key2][key3] for d in results)

    return out


if __name__ == "__main__":
    from argparse import ArgumentParser

    from genutility.rich import Progress
    from rich.progress import Progress as RichProgress

    parser = ArgumentParser()
    parser.add_argument("--outpath", default="bench-dbs", help="Directory to store temporary benchmarking databases")
    parser.add_argument("--version", action="version", version=lmdbm.__version__)
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        metavar="N",
        default=[10, 100, 10**3, 10**4, 10**5, 10**6],
        help="Number of records to read/write",
    )
    parser.add_argument("--bestof", type=int, metavar="N", default=3, help="Run N benchmarks")
    parser.add_argument("--outfile", default="benchmarks.md", help="Benchmark results")
    args = parser.parse_args()

    results: List[ResultsDict] = []

    with RichProgress() as progress:
        p = Progress(progress)
        for _ in p.track(range(args.bestof)):
            results.append(bench(args.outpath, args.sizes))

    if args.bestof == 1:
        best_results = results[0]
    else:
        best_results = merge_results(results)

    with open(args.outfile, "w", encoding="utf-8") as fw:
        write_markdown_table(fw, best_results, "write")
        write_markdown_table(fw, best_results, "batch_write")
        write_markdown_table(fw, best_results, "read")
        write_markdown_table(fw, best_results, "batch_read")
        write_markdown_table(fw, best_results, "combined")


'''
Printout of one run on 2024-06-12


$ python benchmark.py 

bigdict              write           10            0.00111
bigdict              batch write     10            0.00139
bigdict              read            10            0.00261
bigdict              combined        1100          0.01541
lmdbm                write           10            0.01519
lmdbm                batch write     10            0.00220
lmdbm                read            10            0.00034
lmdbm                combined        1100          0.17095

bigdict              write           100           0.00489
bigdict              batch write     100           0.00411
bigdict              read            100           0.00361
bigdict              combined        1100          0.01719
lmdbm                write           100           0.17447
lmdbm                batch write     100           0.00571
lmdbm                read            100           0.00423
lmdbm                combined        1100          0.21392

bigdict              write           1000          0.02298
bigdict              batch write     1000          0.00972
bigdict              read            1000          0.01229
bigdict              combined        1100          0.01342
lmdbm                write           1000          1.98285
lmdbm                batch write     1000          0.02673
lmdbm                read            1000          0.01462
lmdbm                combined        1100          0.22254

bigdict              write           10000         0.12249
bigdict              batch write     10000         0.08896
bigdict              read            10000         0.13044
bigdict              combined        1100          0.01342
lmdbm                write           10000        10.00091
lmdbm                batch write     10000         0.10959
lmdbm                read            10000         0.12810
lmdbm                combined        1100          0.22391

bigdict              write           100000        1.17010
bigdict              batch write     100000        0.89503
bigdict              read            100000        1.35896
bigdict              combined        1100          0.01391
lmdbm                write           100000       10.00036
lmdbm                batch write     100000        0.96608
lmdbm                read            100000        1.29228
lmdbm                combined        1100          0.16843

bigdict              write           1000000      10.00460
bigdict              batch write     1000000       9.00816
bigdict              read            1000000      10.00805
bigdict              combined        1100          0.01634
lmdbm                write           1000000      10.00080
lmdbm                batch write     1000000      10.00143

bigdict              write           10            0.00143
bigdict              batch write     10            0.00269
bigdict              read            10            0.00114
bigdict              combined        1100          0.01327
lmdbm                write           10            0.01820
lmdbm                batch write     10            0.00206
lmdbm                read            10            0.00031
lmdbm                combined        1100          0.23209

bigdict              write           100           0.00508
bigdict              batch write     100           0.00260
bigdict              read            100           0.00251
bigdict              combined        1100          0.01303
lmdbm                write           100           0.19923
lmdbm                batch write     100           0.00637
lmdbm                read            100           0.00518
lmdbm                combined        1100          0.19815

bigdict              write           1000          0.02801
bigdict              batch write     1000          0.00958
bigdict              read            1000          0.01316
bigdict              combined        1100          0.01381
lmdbm                write           1000          2.21595
lmdbm                batch write     1000          0.02330
lmdbm                read            1000          0.01832
lmdbm                combined        1100          0.21415

bigdict              write           10000         0.14018
bigdict              batch write     10000         0.10308
bigdict              read            10000         0.13387
bigdict              combined        1100          0.01628
lmdbm                write           10000        10.00172
lmdbm                batch write     10000         0.10502
lmdbm                read            10000         0.12950
lmdbm                combined        1100          0.18172

bigdict              write           100000        1.20549
bigdict              batch write     100000        1.16667
bigdict              read            100000        1.38001
bigdict              combined        1100          0.01380
lmdbm                write           100000       10.00107
lmdbm                batch write     100000        1.09726
lmdbm                read            100000        1.53972
lmdbm                combined        1100          0.17792

bigdict              write           1000000      10.00386
bigdict              batch write     1000000       9.10241
bigdict              read            1000000      10.00773
bigdict              combined        1100          0.01429
lmdbm                write           1000000      10.00121
lmdbm                batch write     1000000      10.00217

bigdict              write           10            0.00146
bigdict              batch write     10            0.00195
bigdict              read            10            0.00069
bigdict              combined        1100          0.01365
lmdbm                write           10            0.01537
lmdbm                batch write     10            0.00216
lmdbm                read            10            0.00056
lmdbm                combined        1100          0.21139

bigdict              write           100           0.00596
bigdict              batch write     100           0.00529
bigdict              read            100           0.00267
bigdict              combined        1100          0.01484
lmdbm                write           100           0.17925
lmdbm                batch write     100           0.00569
lmdbm                read            100           0.00507
lmdbm                combined        1100          0.22864

bigdict              write           1000          0.02132
bigdict              batch write     1000          0.01195
bigdict              read            1000          0.01108
bigdict              combined        1100          0.01369
lmdbm                write           1000          1.89918
lmdbm                batch write     1000          0.02571
lmdbm                read            1000          0.01625
lmdbm                combined        1100          0.16864

bigdict              write           10000         0.13405
bigdict              batch write     10000         0.08617
bigdict              read            10000         0.12783
bigdict              combined        1100          0.01559
lmdbm                write           10000        10.00167
lmdbm                batch write     10000         0.11594
lmdbm                read            10000         0.13295
lmdbm                combined        1100          0.19025

bigdict              write           100000        1.12400
bigdict              batch write     100000        0.85726
bigdict              read            100000        1.35902
bigdict              combined        1100          0.01511
lmdbm                write           100000       10.00161
lmdbm                batch write     100000        1.02139
lmdbm                read            100000        1.30645
lmdbm                combined        1100          0.17209

bigdict              write           1000000      10.00473
bigdict              batch write     1000000       8.24678
bigdict              read            1000000      10.00765
bigdict              combined        1100          0.01396
lmdbm                write           1000000      10.00123
lmdbm                batch write     1000000       9.58374
lmdbm                read            1000000      10.00463
lmdbm                combined        1100          0.19597
'''