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
BATCH_SIZE = 10000


class BaseBenchmark(ABC):
    def __init__(self, db_tpl, db_type):
        self.available = True
        self.batch_available = True
        self.path = db_tpl.format(db_type)
        self.name = db_type
        self.write = -1
        self.batch = -1
        self.read = -1
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

    def measure_batch(self, N: int) -> None:
        with MeasureTime() as t, self.open() as db:
            for pairs in batch(self.generate_data(N), BATCH_SIZE):
                if t.get() > MAX_TIME:
                    break
                db.update({key: self.encode(value) for key, value in pairs})
                self.commit()
        if t.get() < MAX_TIME:
            self.batch = t.get()
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
        return self.batch >= 0 or self.write >= 0

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

    def open(self):
        if os.path.isdir(self.path):
            return Bigdict(self.path, readonly=False, map_size_mb=1000)
        else:
            return Bigdict.new(self.path, map_size_mb=1000)
    
    def purge(self):
        try:
            Bigdict(self.path, readonly=False).destroy()
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
        if benchmark.batch_available:
            benchmark.purge()
            benchmark.measure_batch(N)
        if benchmark.database_is_built():
            benchmark.measure_reads(N)
            benchmark.measure_combined(read=1, write=10, repeat=100)

    ret: DefaultDict[str, Dict[str, float]] = defaultdict(dict)
    for benchmark in benchmarks:
        ret[benchmark.name]["read"] = benchmark.read
        ret[benchmark.name]["write"] = benchmark.write
        ret[benchmark.name]["batch"] = benchmark.batch
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
        write_markdown_table(fw, best_results, "batch")
        write_markdown_table(fw, best_results, "read")
        write_markdown_table(fw, best_results, "combined")


'''
Printout of one run on 2024-06-11

$ python benchmark.py 

bigdict              write           10            0.00259
bigdict              batch write     10            0.00137
bigdict              read            10            0.00164
bigdict              combined        1100          0.01295
lmdbm                write           10            0.01574
lmdbm                batch write     10            0.00246
lmdbm                read            10            0.00073
lmdbm                combined        1100          0.21779

bigdict              write           100           0.00944
bigdict              batch write     100           0.00886
bigdict              read            100           0.00389
bigdict              combined        1100          0.01584
lmdbm                write           100           0.21064
lmdbm                batch write     100           0.00797
lmdbm                read            100           0.00616
lmdbm                combined        1100          0.18130

bigdict              write           1000          0.02115
bigdict              batch write     1000          0.01319
bigdict              read            1000          0.00949
bigdict              combined        1100          0.01134
lmdbm                write           1000          2.06923
lmdbm                batch write     1000          0.01867
lmdbm                read            1000          0.01468
lmdbm                combined        1100          0.18537

bigdict              write           10000         0.12241
bigdict              batch write     10000         0.08798
bigdict              read            10000         0.11064
bigdict              combined        1100          0.01191
lmdbm                write           10000        10.00024
lmdbm                batch write     10000         0.09944
lmdbm                read            10000         0.13317
lmdbm                combined        1100          0.18213

bigdict              write           100000        1.26246
bigdict              batch write     100000        0.92097
bigdict              read            100000        1.13831
bigdict              combined        1100          0.01368
lmdbm                write           100000       10.00226
lmdbm                batch write     100000        0.83413
lmdbm                read            100000        1.33657
lmdbm                combined        1100          0.17077

bigdict              write           1000000      10.00649
bigdict              batch write     1000000       8.29829
bigdict              read            1000000      10.01007
bigdict              combined        1100          0.01826
lmdbm                write           1000000      10.00180
lmdbm                batch write     1000000       8.15520
lmdbm                read            1000000      10.00381
lmdbm                combined        1100          0.19153

bigdict              write           10            0.00171
bigdict              batch write     10            0.00166
bigdict              read            10            0.00205
bigdict              combined        1100          0.00949
lmdbm                write           10            0.01447
lmdbm                batch write     10            0.00184
lmdbm                read            10            0.00049
lmdbm                combined        1100          0.18549

bigdict              write           100           0.00458
bigdict              batch write     100           0.00393
bigdict              read            100           0.00267
bigdict              combined        1100          0.01455
lmdbm                write           100           0.17768
lmdbm                batch write     100           0.00502
lmdbm                read            100           0.00326
lmdbm                combined        1100          0.22201

bigdict              write           1000          0.02310
bigdict              batch write     1000          0.00879
bigdict              read            1000          0.00898
bigdict              combined        1100          0.01158
lmdbm                write           1000          1.93502
lmdbm                batch write     1000          0.02409
lmdbm                read            1000          0.01462
lmdbm                combined        1100          0.19909

bigdict              write           10000         0.12006
bigdict              batch write     10000         0.08369
bigdict              read            10000         0.10699
bigdict              combined        1100          0.01094
lmdbm                write           10000        10.00131
lmdbm                batch write     10000         0.09939
lmdbm                read            10000         0.12729
lmdbm                combined        1100          0.22036

bigdict              write           100000        1.15446
bigdict              batch write     100000        0.84001
bigdict              read            100000        1.09984
bigdict              combined        1100          0.01300
lmdbm                write           100000       10.00044
lmdbm                batch write     100000        0.83294
lmdbm                read            100000        1.32721
lmdbm                combined        1100          0.16935

bigdict              write           1000000      10.00598
bigdict              batch write     1000000       8.27619
bigdict              read            1000000      10.01036
bigdict              combined        1100          0.01108
lmdbm                write           1000000      10.00137
lmdbm                batch write     1000000       8.01160
lmdbm                read            1000000      10.00307
lmdbm                combined        1100          0.18077

bigdict              write           10            0.00177
bigdict              batch write     10            0.00132
bigdict              read            10            0.00066
bigdict              combined        1100          0.01310
lmdbm                write           10            0.01550
lmdbm                batch write     10            0.00210
lmdbm                read            10            0.00055
lmdbm                combined        1100          0.19435

bigdict              write           100           0.00535
bigdict              batch write     100           0.00320
bigdict              read            100           0.00212
bigdict              combined        1100          0.01270
lmdbm                write           100           0.17157
lmdbm                batch write     100           0.00514
lmdbm                read            100           0.00346
lmdbm                combined        1100          0.20485

bigdict              write           1000          0.02266
bigdict              batch write     1000          0.00960
bigdict              read            1000          0.01374
bigdict              combined        1100          0.01068
lmdbm                write           1000          1.84641
lmdbm                batch write     1000          0.02657
lmdbm                read            1000          0.01394
lmdbm                combined        1100          0.17697

bigdict              write           10000         0.12552
bigdict              batch write     10000         0.08131
bigdict              read            10000         0.10702
bigdict              combined        1100          0.01088
lmdbm                write           10000        10.00138
lmdbm                batch write     10000         0.09720
lmdbm                read            10000         0.12346
lmdbm                combined        1100          0.17387

bigdict              write           100000        1.18787
bigdict              batch write     100000        0.84368
bigdict              read            100000        1.07966
bigdict              combined        1100          0.01139
lmdbm                write           100000       10.00262
lmdbm                batch write     100000        0.80398
lmdbm                read            100000        1.22542
lmdbm                combined        1100          0.20726

bigdict              write           1000000      10.00619
bigdict              batch write     1000000       8.56119
bigdict              read            1000000      10.00991
bigdict              combined        1100          0.01091
lmdbm                write           1000000      10.00136
lmdbm                batch write     1000000       7.92508
lmdbm                read            1000000      10.00378
lmdbm                combined        1100          0.19889
'''
