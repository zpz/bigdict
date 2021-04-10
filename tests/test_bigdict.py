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
