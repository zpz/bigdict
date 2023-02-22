import json
import os
import os.path
import pickle
import shutil
import tempfile
import uuid
from collections.abc import Mapping, MutableMapping

import rocksdb  # type: ignore

# pylint: disable=no-self-use


def rocks_opts(**kwargs):
    # pylint: disable=no-member
    opts = rocksdb.Options(**kwargs)
    opts.table_factory = rocksdb.BlockBasedTableFactory(
        filter_policy=rocksdb.BloomFilterPolicy(10),
        block_cache=rocksdb.LRUCache(2 * (1024**3)),
        block_cache_compressed=rocksdb.LRUCache(500 * (1024**2)),
    )
    return opts


class Bigdict(MutableMapping):
    @classmethod
    def new(
        cls,
        path: str = None,
        *,
        max_log_file_size: int = 2097152,
        keep_log_file_num: int = 2,
    ):
        db_opts = {
            "max_log_file_size": max_log_file_size,
            "keep_log_file_num": keep_log_file_num,
        }
        info = {
            "db_opts": db_opts,
        }

        if path is None:
            keep_files = False
            path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        else:
            keep_files = True
        path = os.path.abspath(path)
        assert not os.path.isdir(path)
        os.makedirs(path)

        _ = rocksdb.DB(  # pylint: disable=no-member
            os.path.join(path, "db"),
            rocks_opts(**db_opts, create_if_missing=True),
        )
        del _

        json.dump(info, open(os.path.join(path, "info.json"), "w"))
        z = cls(path)
        z._keep_files = keep_files  # pylint: disable=protected-access
        return z

    def __init__(
        self,
        path: str,
        read_only: bool = False,
    ):
        self.path = path
        self.info = json.load(open(os.path.join(path, "info.json"), "r"))
        self._db = None
        self._read_only = read_only
        self._keep_files = True
        self._destroyed = False
        self._flushed = True

    def __repr__(self):
        return f"{self.__class__.__name__}({self.path})"

    def __str__(self):
        return self.__repr__()

    def __getstate__(self):
        assert (
            self._read_only
        ), "passing to other processes is supported only for read-only"
        return (self.path,)

    def __setstate__(self, state):
        (self.path,) = state
        self.info = json.load(open(os.path.join(self.path, "info.json"), "r"))
        self._db = None
        self._read_only = True
        self._keep_files = True
        self._destroyed = False
        self._flushed = True

    def encode_key(self, k):
        return pickle.dumps(k)

    def decode_key(self, k):
        return pickle.loads(k)

    def encode_value(self, v):
        return pickle.dumps(v)

    def decode_value(self, v):
        return pickle.loads(v)

    @property
    def db(self):
        if self._db is None:
            self._db = rocksdb.DB(  # pylint: disable=no-member
                os.path.join(self.path, "db"),
                rocks_opts(**self.info["db_opts"], create_if_missing=False),
                read_only=self._read_only,
            )
        return self._db

    def __del__(self):
        if self._destroyed or self._read_only:
            return
        if self._keep_files:
            self.flush()
        else:
            self.destroy()

    def flush(self, compact=True):
        assert not self._read_only
        if self._destroyed or self._flushed:
            return
        json.dump(self.info, open(os.path.join(self.path, "info.json"), "w"))
        if compact:
            self.db.compact_range()
        self._flushed = True

    def clear(self):
        assert not self._read_only
        info = {
            "db_opts": self.info["db_opts"],
        }
        keep_files = self._keep_files
        path = self.path
        shutil.rmtree(self.path)
        os.makedirs(path)
        json.dump(info, open(os.path.join(path, "info.json"), "w"))
        _ = rocksdb.DB(
            os.path.join(path, "db"),  # pylint: disable=no-member
            rocks_opts(**info["db_opts"], create_if_missing=True),
        )
        self.__init__(path)
        self._keep_files = keep_files

    def destroy(self):
        assert not self._read_only
        self._db = None
        shutil.rmtree(self.path, ignore_errors=True)
        self._destroyed = True

    def __setitem__(self, key, value):
        assert not self._read_only
        key = self.encode_key(key)
        value = self.encode_value(value)
        self.db.put(key, value)
        self._flushed = False

    def __getitem__(self, key):
        byteskey = self.encode_key(key)
        value = self.db.get(byteskey)
        if value is None:
            raise KeyError(key)
        return self.decode_value(value)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        it = self.db.iterkeys()
        it.seek_to_first()
        for key in it:
            yield self.decode_key(key)

    def values(self):
        it = self.db.itervalues()
        it.seek_to_first()
        for value in it:
            yield self.decode_value(value)

    def __iter__(self):
        return self.keys()

    def items(self):
        it = self.db.iteritems()
        it.seek_to_first()
        for key, value in it:
            yield self.decode_key(key), self.decode_value(value)

    def __contains__(self, key):
        try:
            _ = self[key]
            return True
        except KeyError:
            return False

    def __len__(self) -> int:
        count = 0
        it = self.db.iterkeys()
        it.seek_to_first()
        for _ in it:
            count += 1
        return count

    def __bool__(self) -> bool:
        return self.__len__() > 0

    def __delitem__(self, key):
        assert not self._read_only
        self.db.delete(self.encode_key(key))
        self._flushed = False

    def view(self) -> "DictView":
        return DictView(self.__class__(self.path, read_only=True))


class DictView(Mapping):
    # `types.MappingProxyType` could achieve similar effects,
    # but it might be problematic when we send the object
    # to another process. Plus, this separate class will have
    # any flexibility that may be needed.

    def __init__(self, dict_: Mapping):
        self._dict = dict_

    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self._dict)})"

    def __str__(self):
        return self.__repr__()

    def __getitem__(self, key):
        return self._dict[key]

    def get(self, key, default=None):
        return self._dict.get(key, default)

    def keys(self):
        return self._dict.keys()

    def values(self):
        return self._dict.values()

    def __iter__(self):
        return self._dict.__iter__()

    def items(self):
        return self._dict.items()

    def __contains__(self, key):
        return self._dict.__contains__(key)

    def __len__(self):
        return self._dict.__len__()

    def __bool__(self):
        return self._dict.__bool__()
