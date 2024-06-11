import multiprocessing
import pickle
import queue
import threading
from time import sleep
from uuid import uuid4

import pytest
from bigdict import Bigdict, ReadonlyError


def test_basics():
    bd: Bigdict = Bigdict.new(map_size_mb=64)
    print(bd)

    assert list(bd.keys()) == []
    assert list(bd.values()) == []
    assert list(bd.items()) == []

    bd["a"] = 3
    bd["9"] = [1, 2, "a"]
    bd["b"] = {"a": 3, "b": 4}
    uid = str(uuid4())
    bd["uid"] = uid

    assert bd["a"] == 3
    assert bd.setdefault("a", 4) == 3
    assert bd.setdefault("c", 4) == 4

    assert bd["a"] == 3

    del bd["a"]
    assert "a" not in bd

    assert len(bd) == 4

    assert bd.setdefault("a", 5) == 5
    assert bd["c"] == 4
    assert len(bd) == 5

    bd.update(a="a")
    assert bd["a"] == "a"

    with pytest.raises(KeyError):
        assert bd["g"] == 8

    assert bd.get("g", 999) == 999

    assert bd.pop("a") == "a"
    assert "a" not in bd

    bd2 = Bigdict(bd.path, map_size_mb=32, readonly=True)
    # apparently `map_size` is not an attribute of the database file---you
    # can choose another `map_size` when reading.

    assert bd2["c"] == 4
    assert bd2["9"] == [1, 2, "a"]
    assert bd2["b"] == {"a": 3, "b": 4}
    assert bd2["uid"] == uid

    del bd["c"]
    assert "c" not in bd

    # Before `bd` commit, the other reader does not see the change.
    assert bd2["c"] == 4
    assert "c" in bd2

    bd.commit()
    # After writer commit, the other reader sees the changes.
    with pytest.raises(KeyError):
        assert bd2["c"] == 4

    with pytest.raises(KeyError):
        del bd["99"]

    with pytest.raises(KeyError):
        bd.pop("99")

    assert len(bd) == 3
    assert len(bd2) == 3

    assert sorted(bd.keys()) == sorted(["9", "b", "uid"])

    with pytest.raises(ReadonlyError):
        bd2.destroy()

    with pytest.raises(ReadonlyError):
        bd2["f"] = 3

    with pytest.raises(ReadonlyError):
        bd2.pop("a")

    with pytest.raises(ReadonlyError):
        bd2.setdefault("a", 2)

    with pytest.raises(ReadonlyError):
        del bd2["b"]

    with pytest.raises(ReadonlyError):
        bd2.update([("z", 100)])

    with pytest.raises(ReadonlyError):
        bd2.commit()

    bd.destroy()


def test_pickle():
    data = Bigdict.new()
    data["1"] = 3
    data["2"] = "b"
    data.flush()

    dd = pickle.dumps(data)
    data2 = pickle.loads(dd)
    # `data2` and `data` can co-exist between the latter is readonly.

    assert len(data2) == 2
    assert data2["1"] == 3
    assert data2["2"] == "b"


def mp_worker(path, map_size_mb, q):
    d = Bigdict(path, map_size_mb=map_size_mb, readonly=False)

    assert len(d) == 1
    assert d["a"] == 3
    assert "b" not in d

    d.commit()

    q.put(1)
    sleep(0.2)

    assert q.get() == 2

    assert d["b"] == 4

    assert len(d) == 2

    d["c"] = 9
    d.commit()
    q.put(3)


def test_mp():
    for executor in ("thread", "process"):
        print("executor:", executor)

        bd = Bigdict.new()
        bd["a"] = 3
        bd.commit()

        if executor == "thread":
            q = queue.Queue()
            cls = threading.Thread
        else:
            ctx = multiprocessing.get_context("spawn")
            q = ctx.Queue()
            cls = ctx.Process

        task = cls(target=mp_worker, args=(bd.path, bd.map_size_mb, q))
        task.start()

        assert q.get() == 1

        bd["b"] = 4
        bd.commit()

        q.put(2)
        sleep(0.2)

        assert q.get() == 3

        assert bd["c"] == 9

        task.join()
        bd.destroy()


def th_worker(data, e1, e2):
    # data = Bigdict(path, readonly=True)

    assert len(data) == 2
    assert data["1"] == "a"
    data.commit()

    e2.set()
    e1.wait()

    # data["3"] = "c"
    # data.commit()

    assert data["4"] == "d"
    assert "3" not in data
    assert len(data) == 3


def test_thread():
    data = Bigdict.new()
    data["1"] = "a"
    data["2"] = "b"
    data.flush()

    e1, e2 = threading.Event(), threading.Event()

    task = threading.Thread(target=th_worker, args=(data, e1, e2))
    task.start()

    e2.wait()

    assert "3" not in data
    data["4"] = "d"

    assert "4" in data
    data.commit()
    # assert data["3"] == "c"
    assert data["4"] == "d"
    e1.set()

    task.join()
    data.destroy()


def test_destroy():
    data = Bigdict.new()
    data["1"] = "a"
    data["2"] = "b"
    data.flush()
    data.destroy()


def test_shard():
    N = 10000
    db = Bigdict.new(shard_level=16)
    print()

    data = [str(uuid4()) for _ in range(N)]
    for d in data:
        db[d] = d

    db.flush()

    assert len(db._shards()) == 16

    assert len(db) == N

    for d in data:
        assert db[d] == d

    assert sorted(data) == sorted(db)  # calls `db.keys()`
    assert sorted(data) == sorted(db.values())

    db.compact()
    print(db._num_pending_writes)

    assert not db._dbs
    assert not db._transactions

    assert sorted(data) == sorted(db)  # calls `db.keys()`
    assert sorted(data) == sorted(db.values())

    db2 = Bigdict(db.path, map_size_mb=10)
    assert sorted(data) == sorted(db2)  # calls `db.keys()`
    assert sorted(data) == sorted(db2.values())

    db3 = Bigdict(db.path, map_size_mb=100)
    assert sorted(data) == sorted(db3)  # calls `db.keys()`
    assert sorted(data) == sorted(db3.values())

    db.destroy()
