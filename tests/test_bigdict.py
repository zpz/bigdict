import multiprocessing
from time import sleep
from uuid import uuid4

from bigdict import Bigdict


def test_bigdict():
    bd = Bigdict.new()
    print(bd)

    bd['a'] = 3
    bd['b'] = 4
    bd[9] = [1, 2, 'a']
    bd[('a', 3)] = {'a': 3, 'b': 4}
    uid = str(uuid4())
    bd['uid'] = uid

    assert len(bd) == 5

    bd2 = bd.view()
    assert bd2['a'] == 3
    assert bd2['b'] == 4
    assert bd2[9] == [1, 2, 'a']
    assert bd2[('a', 3)] == {'a': 3, 'b': 4}
    assert bd2['uid'] == uid

    del bd['b']
    assert 'b' not in bd
    assert len(bd) == 4
    bd.flush()

    assert len(bd2) == 5
    bd.destroy()


def worker(d, q):
    assert d['a'] == 3
    assert 'b' not in d
    q.put(1)
    assert q.get() == 2
    # assert d['b'] == 4
    # 'b' is not available


def test_mp():
    bd = Bigdict.new()
    bd['a'] = 3
    bdr = Bigdict(bd.path, read_only=True)

    mp = multiprocessing.get_context('spawn')
    q = mp.Queue()
    p = mp.Process(target=worker, args=(bdr, q))
    p.start()

    assert q.get() == 1
    bd['b'] = 4
    bd.flush()
    sleep(1)
    q.put(2)

    p.join()

    assert bd['b'] == 4
