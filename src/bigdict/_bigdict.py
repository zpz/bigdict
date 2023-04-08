import json
import os
import os.path
import pickle
import shutil
import tempfile
import uuid
import warnings
from collections.abc import MutableMapping

import lmdb
import rocksdb


def rocks_opts(**kwargs):
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
    ):
        info = {
            "storage_version": 1,
            # `storage_version = 1` is introduced in release 0.2.0.
            # It was missing before that, and treated as 0.
            # Version 0 used a RocksDB backend;
            # version 1 uses a LMDB backend.
            "shard_level": 0,
            # Other values will be allowed later.
        }

        if path is None:
            keep_files = False
            path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        else:
            keep_files = True
        path = os.path.abspath(path)
        assert not os.path.isdir(path)
        os.makedirs(path)

        json.dump(info, open(os.path.join(path, "info.json"), "w"))
        z = cls(path, read_only=False)
        z._keep_files = keep_files
        return z

    def __init__(
        self,
        path: str,
        read_only: bool = False,
    ):
        self.path = path
        self.info = json.load(open(os.path.join(path, "info.json"), "r"))
        if self.info.get('storage_version', 0) == 0:
            if not read_only:
                warnings.warn("older data with RocksDB backend is read-only")
                read_only = True
        self._db = None
        self._wtxn = None  # write transaction
        self._rtxn = None  # read transaction
        self._read_only = read_only
        self._keep_files = True
        self._destroyed = False
        self._flushed = True
        self._storage_version = self.info.get('storage_version', 0)

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
        self._wtxn = None
        self._rtxn = None
        self._read_only = True
        self._keep_files = True
        self._destroyed = False
        self._flushed = True
        self._storage_version = self.info.get('storage_version', 0)

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
            if self._storage_version == 0:
                self._db = rocksdb.DB(  # pylint: disable=no-member
                    os.path.join(self.path, "db"),
                    rocks_opts(**self.info["db_opts"], create_if_missing=False),
                    read_only=self._read_only,
                )
            else:
                if self._read_only:
                    self._db = lmdb.Environment(
                        os.path.join(self.path, 'db', '0'),
                        subdir=True,
                        create=False,
                        readonly=True,
                        readahead=False,
                    )
                else:
                    os.makedirs(os.path.join(self.path, 'db', '0'), exist_ok=True)
                    self._db = lmdb.Environment(
                        os.path.join(self.path, 'db', '0'),
                        subdir=True,
                        readonly=False,
                        writemap=True,
                        readahead=False,
                    )
        return self._db

    @property
    def _write_txn(self):
        if self._wtxn is None:
            txn = lmdb.Transaction(self.db, write=True)
            txn.__enter__()
            self._wtxn = txn
        return self._wtxn

    @property
    def _read_txn(self):
        if self._rtxn is None:
            txn = lmdb.Transaction(self.db, write=False)
            txn.__enter__()
            self._rtxn = txn
        return self._rtxn

    def _close(self):
        if self._storage_version == 0:
            return
        if self._wtxn is not None:
            self._wtxn.__exit__()
            self._write_txn = None
        if self._rtxn is not None:
            self._rtxn.__exit__()
            self._rtxn = None
        if self._db is not None:
            self._db.close()
            self._db = None

    def __del__(self):
        self._close()
        if self._destroyed or self._read_only:
            return
        if self._keep_files:
            self.flush()
        else:
            self.destroy()

    def commit(self):
        if self._wtxn is not None:
            self._wtxn.commit()
            self._wtxn = None

    def flush(self):
        assert not self._read_only
        if self._destroyed or self._flushed:
            return
        self._close()
        json.dump(self.info, open(os.path.join(self.path, "info.json"), "w"))
        self._flushed = True

    def clear(self):
        assert not self._read_only
        self._close()
        info = {
            'storage_version': self.info['storage_version'],
        }
        keep_files = self._keep_files
        path = self.path
        shutil.rmtree(self.path)
        os.makedirs(path)
        json.dump(info, open(os.path.join(path, "info.json"), "w"))
        self.__init__(path)
        self._keep_files = keep_files

    def destroy(self):
        assert not self._read_only
        self._close()
        shutil.rmtree(self.path, ignore_errors=True)
        self._destroyed = True

    def __setitem__(self, key, value):
        assert not self._read_only
        key = self.encode_key(key)
        value = self.encode_value(value)
        self._write_txn.put(key, value)
        self._flushed = False

    def __getitem__(self, key):
        byteskey = self.encode_key(key)
        if self._storage_version == 0:
            value = self.db.get(byteskey)
        else:
            value = self._read_txn.get(byteskey)
        # `value` can't be `None` as a valid return from the db,
        # because all values are bytes.
        if value is None:
            raise KeyError(key)
        return self.decode_value(value)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        if self._storage_version == 0:
            it = self.db.iterkeys()
            it.seek_to_first()
            for key in it:
                yield self.decode_key(key)
        else:
            cursor = self._read_txn.cursor()
            for k in cursor.iternext(keys=True, values=False):
                yield k

    def values(self):
        if self._storage_version == 0:
            it = self.db.itervalues()
            it.seek_to_first()
            for value in it:
                yield self.decode_value(value)
        else:
            cursor = self._read_txn.cursor()
            for v in cursor.iternext(keys=False, values=True):
                yield v

    def __iter__(self):
        return self.keys()

    def items(self):
        if self._storage_version == 0:
            it = self.db.iteritems()
            it.seek_to_first()
            for key, value in it:
                yield self.decode_key(key), self.decode_value(value)
        else:
            cursor = self._read_txn.cursor()
            for key, value in cursor:
                yield self.decode_key(key), self.decode_value(value)

    def __contains__(self, key):
        try:
            _ = self[key]
            return True
        except KeyError:
            return False

    def __len__(self) -> int:
        if self._storage_version == 0:
            count = 0
            it = self.db.iterkeys()
            it.seek_to_first()
            for _ in it:
                count += 1
            return count
        stat = self.db.stat()
        return stat['entries']

    def __bool__(self) -> bool:
        return self.__len__() > 0

    def __delitem__(self, key):
        assert not self._read_only
        z = self._write_txn.delete(self.encode_key(key))
        if not z:
            raise KeyError(key)
        self._flushed = False
