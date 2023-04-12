import json
import os
import os.path
import pickle
import shutil
import tempfile
import uuid
import warnings

import lmdb

UNSET = object()


class Bigdict:
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

        self._storage_version = self.info.get('storage_version', 0)
        if self._storage_version == 0:
            warnings.warn(
                "Support for RocksDB storage is deprecated. Please migrate this old dataset to the new format."
            )
            if not read_only:
                warnings.warn("older data with RocksDB backend is read-only")
                read_only = True
            self._key_pickle_protocol = 4
        else:
            self._key_pickle_protocol = 5

        self.read_only = read_only
        self._keep_files = True

        self._dbs = {}  # environments
        self._wtxns = {}  # write transactions
        self._rtxns = {}  # read transactions

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.path}')"

    def __str__(self):
        return self.__repr__()

    def __getstate__(self):
        return self.path, self.info, self.read_only, self._keep_files

    def __setstate__(self, state):
        self.path, self.info, self.read_only, self._keep_files = state
        self._storage_version = self.info.get('storage_version', 0)
        if self._storage_version == 0:
            self._key_pickle_protocol = 4
        else:
            self._key_pickle_protocol = 5
        self._dbs = {}
        self._wtxns = {}
        self._rtxns = {}

    def encode_key(self, k) -> bytes:
        '''
        As a general principle, do not persist pickled custom class objects.
        If ``k`` is not of a "native" Python class like str, dict, etc.,
        subclass should customize this method to convert ``k`` to a native type
        before pickling (or convert to bytes in a whole diff way w/o pickling).
        Correspnoding customization should happen in :meth:`decode_key`.
        '''
        # If reading an existing dataset, the key must be pickled by the same protocol
        # that was used in the original writing, otherwise the key can not be found.
        # That's why we fix the protocol here.
        return pickle.dumps(k, protocol=self._key_pickle_protocol)

    def decode_key(self, k: bytes):
        return pickle.loads(k)

    def encode_value(self, v) -> bytes:
        '''

        As a general principle, do not persist pickled custom class objects.
        If ``v`` is not of a "native" Python class like str, dict, etc.,
        subclass should customize this method to convert ``v`` to a native type
        before pickling (or convert to bytes in a whole diff way w/o pickling).
        Correspnoding customization should happen in :meth:`decode_value`.
        '''
        return pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)

    def decode_value(self, v: bytes):
        return pickle.loads(v)

    def _shard(self, key: bytes) -> str:
        # When `shard_level` supports >0 values,
        # this could return other values.
        return '0'

    def _db(self, shard: str = '0'):
        if self._storage_version == 0:
            if not self._dbs.get('0'):
                import rocksdb

                def rocks_opts(**kwargs):
                    opts = rocksdb.Options(**kwargs)
                    opts.table_factory = rocksdb.BlockBasedTableFactory(
                        filter_policy=rocksdb.BloomFilterPolicy(10),
                        block_cache=rocksdb.LRUCache(2 * (1024**3)),
                        block_cache_compressed=rocksdb.LRUCache(500 * (1024**2)),
                    )
                    return opts

                self._dbs['0'] = rocksdb.DB(  # pylint: disable=no-member
                    os.path.join(self.path, "db"),
                    rocks_opts(**self.info["db_opts"], create_if_missing=False),
                    read_only=self.read_only,
                )
            return self._dbs['0']

        db = self._dbs.get(shard, None)
        if db is None:
            if self.read_only:
                db = lmdb.Environment(
                    os.path.join(self.path, 'db', shard),
                    subdir=True,
                    create=False,
                    readonly=True,
                    readahead=False,
                )
            else:
                os.makedirs(os.path.join(self.path, 'db', shard), exist_ok=True)
                db = lmdb.Environment(
                    os.path.join(self.path, 'db', shard),
                    subdir=True,
                    readonly=False,
                    writemap=True,
                    readahead=False,
                )
            self._dbs[shard] = db
        return db

    def _write_txn(self, shard: str = '0'):
        if shard not in self._wtxns:
            txn = lmdb.Transaction(self._db(shard), write=True)
            txn.__enter__()
            self._wtxns[shard] = txn
        return self._wtxns[shard]

    def _read_txn(self, shard: str = '0'):
        if shard not in self._rtxns:
            txn = lmdb.Transaction(self._db(shard), write=False)
            txn.__enter__()
            self._rtxns[shard] = txn
        return self._rtxns[shard]

    def _commit(self):
        for x in self._wtxns.values():
            x.commit()
        self._wtxns = {}

    def _close(self):
        if self._storage_version == 0:
            return
        for x in self._wtxns.values():
            # x.abort()
            x.__exit__()
        self._wtxns = {}
        for x in self._rtxns.values():
            # x.abort()
            x.__exit__()
        self._rtxns = {}
        for x in self._dbs.values():
            x.close()
        self._dbs = {}

    def __del__(self):
        if self.read_only:
            return
        if self._keep_files:
            self.flush()
        else:
            self.destroy()

    def __setitem__(self, key, value):
        assert not self.read_only
        key = self.encode_key(key)
        shard = self._shard(key)
        value = self.encode_value(value)
        self._write_txn(shard).put(key, value)

    def __getitem__(self, key):
        k = self.encode_key(key)
        if self._storage_version == 0:
            v = self._db().get(k)
        else:
            shard = self._shard(k)
            v = self._read_txn(shard).get(k)
        # `v` can't be `None` as a valid return from the db,
        # because all values are bytes.
        if v is None:
            raise KeyError(key)
        return self.decode_value(v)

    def __delitem__(self, key):
        assert not self.read_only
        k = self.encode_key(key)
        shard = self._shard(k)
        z = self._write_txn(shard).delete(k)
        if not z:
            raise KeyError(key)

    def pop(self, key, default=UNSET):
        k = self.encode_key(key)
        shard = self._shard(k)
        v = self._write_txn(shard).pop(k)
        if v is None:
            if default is UNSET:
                raise KeyError(key)
            return default
        value = self.decode_value(v)
        return value

    def setdefault(self, key, value):
        try:
            return self[key]
        except KeyError:
            self[key] = value
            return value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        if self._storage_version == 0:
            it = self._db().iterkeys()
            it.seek_to_first()
            for key in it:
                yield self.decode_key(key)
        else:
            # TODO: loop through all shards
            cursor = self._read_txn('0').cursor()
            for k in cursor.iternext(keys=True, values=False):
                yield self.decode_key(k)

    def values(self):
        if self._storage_version == 0:
            it = self._db().itervalues()
            it.seek_to_first()
            for value in it:
                yield self.decode_value(value)
        else:
            # TODO: loop through all shards
            cursor = self._read_txn('0').cursor()
            for v in cursor.iternext(keys=False, values=True):
                yield self.decode_value(v)

    def __iter__(self):
        return self.keys()

    def items(self):
        if self._storage_version == 0:
            it = self._db().iteritems()
            it.seek_to_first()
            for key, value in it:
                yield self.decode_key(key), self.decode_value(value)
        else:
            # TODO: loop through all shards
            cursor = self._read_txn('0').cursor()
            for key, value in cursor.iternext(keys=True, values=True):
                yield self.decode_key(key), self.decode_value(value)

    def __contains__(self, key):
        try:
            _ = self.__getitem__(key)
            return True
        except KeyError:
            return False

    def __len__(self) -> int:
        if self._storage_version == 0:
            count = 0
            it = self._db().iterkeys()
            it.seek_to_first()
            for _ in it:
                count += 1
            return count

        # TODO: loop through all shards
        stat = self._db('0').stat()
        return stat['entries']

    def __bool__(self) -> bool:
        return self.__len__() > 0

    def commit(self):
        '''
        Commit and close all pending transactions.

        :meth:`flush` is this ``commit`` plus saving the info file.
        If you know ``self.info`` has not changed and the overhead of saving
        the info file is significant in your use case (because for some reason
        you need to commit writes frequently), you can call ``commit`` instead
        of ``flush``.
        '''
        self._commit()
        self._close()

    def flush(self):
        '''
        ``flush`` commits all writes (set/update/delete), and saves ``self.info``.
        Before ``flush`` is called, recent writes may not be available to reading.

        Do not call this after every write; instead, call this after a write "session",
        before you'done writing or you need to read.
        '''
        assert not self.read_only
        self.commit()
        json.dump(self.info, open(os.path.join(self.path, "info.json"), "w"))

    def destroy(self):
        '''
        After ``destroy``, disk data is erased and the object is no longer usable.
        '''
        assert not self.read_only
        self._close()
        shutil.rmtree(self.path, ignore_errors=True)
        try:
            delattr(self, 'info')
        except AttributeError:
            pass

    def reload(self):
        '''
        Call this on a reader object to pick up any recent writes performed
        by another writer object since the creation of the reader object.
        '''
        self._close()
        self.info = json.load(open(os.path.join(self.path, "info.json"), "r"))
