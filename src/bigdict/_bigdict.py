from __future__ import annotations

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
        *,
        keep_files: bool | None = None,
        shard_level: int = 0,
        **kwargs,
    ):
        '''
        Parameters
        ----------
        shard_level
            Indicates how many "shards"" will be used to store the data.
            Default ``0`` means a single one; ``1`` has the same effect.
            Other accepted values include ``8``, ``16``, ``32``, ``64``, ``128``, ``256``.

            The shards will be named "0", "1", "2", etc.
            They are directories under ``self.path / 'db'``.
            Data elements will be assigned to the shards in a deterministic
            and hopefully balanced way.
        '''
        assert shard_level in (0, 1, 8, 16, 32, 64, 128, 256)
        info = {
            "storage_version": 1,
            # `storage_version = 1` is introduced in release 0.2.0.
            # It was missing before that, and treated as 0.
            # Version 0 used a RocksDB backend;
            # version 1 uses a LMDB backend.
            "shard_level": shard_level,
        }

        if path is None:
            if keep_files is None:
                keep_files = False
            path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        else:
            if keep_files is None:
                keep_files = True
        path = os.path.abspath(path)
        assert not os.path.isdir(path)
        os.makedirs(path)

        json.dump(info, open(os.path.join(path, "info.json"), "w"))
        z = cls(path, read_only=False, **kwargs)
        z._keep_files = keep_files
        return z

    def __init__(
        self,
        path: str,
        *,
        read_only: bool = False,
        lmdb_env_config: dict = None,
    ):
        '''
        Parameters
        ----------
        lmdb_env_config
            Additional named arguments to `lmdb.Environment <https://lmdb.readthedocs.io/en/release/#lmdb.Environment>`_
            for experimentations.
        '''
        self.path = os.path.abspath(path)

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

        self._shard_level = self.info.get('shard_level', 0)
        # DO NOT EVER manually modify ``self._storage_version`` and ``self._shard_level``.

        self.read_only = read_only
        self._keep_files = True

        self._lmdb_env_config = {
            'subdir': True,
            'readahead': False,
            'map_size': 67108864,  # 64 MB; 1073741824 is 2**30, or 1GB
            **(lmdb_env_config or {}),
        }

        # The size of the file `self.path / 'db' / '0' / 'data.mdb'` will display
        # the `map_size` value; I don't know whether it's really the physical size
        # even if we haven't put much data in it.

        self._dbs = {}  # environments
        self._wtxns = {}  # write transactions
        self._rtxns = {}  # read transactions

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.path}')"

    def __str__(self):
        return self.__repr__()

    def __getstate__(self):
        return (
            self.path,
            self.info,
            self.read_only,
            self._keep_files,
            self._lmdb_env_config,
        )

    def __setstate__(self, state):
        (
            self.path,
            self.info,
            self.read_only,
            self._keep_files,
            self._lmdb_env_config,
        ) = state
        self._storage_version = self.info.get('storage_version', 0)
        if self._storage_version == 0:
            self._key_pickle_protocol = 4
        else:
            self._key_pickle_protocol = 5
        self._shard_level = self.info.get('shard_level', 0)
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

        If ``k`` is str, you may want to override ``encode_key`` and ``decode_key``
        to use string ``decode``/``encode`` rather than pickling.
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

    def _shards(self) -> list[str]:
        if self._shard_level <= 1:
            return ['0']

        files = []
        try:
            for f in os.listdir(os.path.join(self.path, 'db')):
                try:
                    f = int(f)
                except ValueError:
                    pass
                else:
                    files.append(f)
            if files:
                files = [str(f) for f in sorted(files)]
        except (FileNotFoundError, NotADirectoryError):
            pass
        return files

    def _shard(self, key: bytes) -> str:
        # When `shard_level` supports >0 values,
        # this could return other values.
        sv = self._storage_version
        sl = self._shard_level
        if sv < 1 or sl <= 1:
            return '0'
        if sv == 1:
            if len(key) == 0:  # TODO: should we allow empty key value?
                return '0'
            base = hash(key)  # TODO: is ``hash`` stable across Python versions?
            if sl == 8:
                base &= 0b111  # keep the right-most 3 bits, 0 ~ 7
            elif sl == 16:
                base &= 0b1111  # keep the right-most 4 bits, 0 ~ 15
            elif sl == 32:
                base &= 0b11111  # keep the right-most 5 bits, 0 ~ 31
            elif sl == 64:
                base &= 0b111111  # keep the right-most 6 bits, 0 ~ 63
            elif sl == 128:
                base &= 0b1111111  # keep the right-most 7 bits, 0 ~ 127
            elif sl == 256:
                pass  # keep all 8 bits, 0 ~ 255
            else:
                raise ValueError(f"shard-level {sl}")
            return str(int(base))
        return ValueError(f"storage-version {sv}")

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
                    create=False,
                    readonly=True,
                    **self._lmdb_env_config,
                )
            else:
                os.makedirs(os.path.join(self.path, 'db', shard), exist_ok=True)
                db = lmdb.Environment(
                    os.path.join(self.path, 'db', shard),
                    readonly=False,
                    writemap=True,
                    **self._lmdb_env_config,
                )
            self._dbs[shard] = db
        return db

    def _write_txn(self, shard: str = '0'):
        # TODO: check out the `buffers` parameter to ``lmdb.Transaction``.
        if shard not in self._wtxns:
            txn = lmdb.Transaction(self._db(shard), write=True)
            txn.__enter__()
            self._wtxns[shard] = txn
        return self._wtxns[shard]

    def _read_txn(self, shard: str = '0'):
        # TODO: check out the `buffers` parameter to ``lmdb.Transaction``.
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
            for shard in self._shards():
                cursor = self._read_txn(shard).cursor()
                for k in cursor.iternext(keys=True, values=False):
                    yield self.decode_key(k)

    def values(self):
        if self._storage_version == 0:
            it = self._db().itervalues()
            it.seek_to_first()
            for value in it:
                yield self.decode_value(value)
        else:
            for shard in self._shards():
                cursor = self._read_txn(shard).cursor()
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
            for shard in self._shards():
                cursor = self._read_txn(shard).cursor()
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

        n = 0
        for shard in self._shards():
            stat = self._db(shard).stat()
            n += stat['entries']
        return n

    def __bool__(self) -> bool:
        if self._storage_version == 0:
            it = self._db().iterkeys()
            it.seek_to_first()
            for _ in it:
                return True
            return False

        for shard in self._shards():
            stat = self._db(shard).stat()
            n = stat['entries']
            if n:
                return True
        return False

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

    def rollback(self):
        '''
        Rollback un-committed write transactions.

        Note: this does not affect changes to ``self.info``.
        '''
        for x in self._wtxns.values():
            x.abort()
        self._wtxns = {}

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
        self._keep_files = False  # to prevent issues in subsequent ``__del__``.

    def reload(self):
        '''
        Call this on a reader object to pick up any recent writes performed
        by another writer object since the creation of the reader object.
        '''
        self._close()
        self.info = json.load(open(os.path.join(self.path, "info.json"), "r"))
