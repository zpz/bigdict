import multiprocessing
import os
import pickle
import queue
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from uuid import uuid4

import pytest
from bigdict import Bigdict


def test_bigdict():
    bd = Bigdict.new()
    print(bd)

    bd['a'] = 3
    bd['b'] = 4
    bd.destroy()

    bd = Bigdict.new()
    bd['a'] = 3
    bd[9] = [1, 2, 'a']
    bd[('a', 3)] = {'a': 3, 'b': 4}
    uid = str(uuid4())
    bd['uid'] = uid

    assert (
        bd.setdefault('a', 4) == 4
    )  # the un-commited 'a' is invisible and hence overwritten
    assert bd.setdefault('b', 4) == 4

    with pytest.raises(KeyError):
        # Not available before flush:
        assert bd['a'] == 3

    # Length is not correct before flush:
    assert len(bd) == 0

    bd.flush()
    assert bd['a'] == 4
    assert bd.setdefault('a', 5) == 4
    assert bd['b'] == 4
    assert len(bd) == 5

    bd['a'] = 'a'
    bd.flush()
    assert bd['a'] == 'a'

    with pytest.raises(KeyError):
        assert bd['g'] == 8

    assert bd.get('g', 999) == 999

    assert bd.pop('a') == 'a'
    assert bd['a'] == 'a'
    bd.flush()
    assert 'a' not in bd

    bd2 = Bigdict(bd.path, read_only=True)
    assert bd2['b'] == 4
    assert bd2[9] == [1, 2, 'a']
    assert bd2[('a', 3)] == {'a': 3, 'b': 4}
    assert bd2['uid'] == uid

    del bd['b']
    bd.flush()

    with pytest.raises(KeyError):
        del bd['99']

    with pytest.raises(KeyError):
        bd.pop('99')

    assert 'b' not in bd
    assert len(bd) == 3

    assert sorted(bd.keys(), key=lambda k: bd.encode_key(k)) == sorted(
        [9, ('a', 3), 'uid'], key=lambda k: bd.encode_key(k)
    )

    # deletion is not reflected in the other reader:
    assert bd2['b'] == 4
    assert 'b' in bd2
    # but length is somehow queries on demand:
    assert len(bd2) == 3

    bd2.reload()
    assert 'b' not in bd2
    with pytest.raises(KeyError):
        assert bd2['b'] == 4

    assert len(bd2) == 3
    bd.destroy()


def test_rollback():
    db = Bigdict.new()
    db['a'] = 3
    db['b'] = 4
    db.flush()

    try:
        db['c'] = 9
        db['d'] = 10
        raise ValueError(3)
    except Exception:
        db.rollback()
    else:
        db.commit()

    assert 'c' not in db
    assert 'd' not in db
    db.commit()
    assert 'c' not in db
    assert 'd' not in db

    db['c'] = 9
    db['d'] = 10
    db.commit()
    assert 'c' in db
    db.rollback()
    assert 'd' in db


def test_compat():
    # Read an older dataset persisted by the RocksDB backend.
    p = os.path.join(os.path.dirname(__file__), 'data010')
    data = Bigdict(p)
    print(list(data.keys()))
    print(list(data.values()))
    print(list(data.items()))
    assert data.read_only
    assert data._storage_version == 0
    assert len(data) == 5
    assert data[9] == [1, 2, 'a']
    assert data['a'] == 3
    assert data['b'] == 4
    assert data['uid'] == 'abcde'
    assert data[('a', 3)] == {'a': 3, 'b': 4}


def test_pickle():
    data = Bigdict.new()
    data[1] = 3
    data[2] = 'b'
    data.flush()

    dd = pickle.dumps(data)
    data2 = pickle.loads(dd)
    assert len(data2) == 2
    assert data2[1] == 3
    assert data2[2] == 'b'


def mp_worker(d, q):
    assert d['a'] == 3
    assert 'b' not in d
    assert len(d) == 1
    q.put(1)
    assert q.get() == 2

    # new write is not visible until reload
    assert 'b' not in d

    # current length is accurate w/o reload
    assert len(d) == 2

    d.reload()
    assert 'b' in d
    assert len(d) == 2


def test_mp():
    bd = Bigdict.new()
    bd['a'] = 3
    bd.flush()

    ctx = multiprocessing.get_context('spawn')
    q = ctx.Queue()
    task = ctx.Process(target=mp_worker, args=(bd, q))
    task.start()
    assert q.get() == 1
    bd['b'] = 4
    bd.flush()
    sleep(1)
    q.put(2)

    task.join()
    assert task.exitcode == 0


def th_worker(data, q):
    assert len(data) == 2
    data[3] = 'c'
    q.put(1)
    sleep(0.2)
    assert q.get() == 1
    assert data[4] == 'd'
    assert len(data) == 4


def test_thread():
    data = Bigdict.new()
    data[1] = 'a'
    data[2] = 'b'
    data.flush()

    # Main thread and the child thread share write/read transactions.
    # But the sleep durations are tricky.

    q = queue.Queue()
    with ThreadPoolExecutor(1) as pool:
        task = pool.submit(th_worker, data, q)
        sleep(0.1)
        assert q.get() == 1
        assert 3 not in data
        data[4] = 'd'
        assert 4 not in data
        data.flush()
        assert data[3] == 'c'
        assert data[4] == 'd'
        q.put(1)
        sleep(0.1)

        task.result()
