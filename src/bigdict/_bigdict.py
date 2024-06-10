from __future__ import annotations

import json
import os
import os.path
import pickle
import shutil
import tempfile
import uuid
from collections import defaultdict
from collections.abc import Iterator, MutableMapping, Iterable, Mapping
from hashlib import blake2b
from typing import Generic, TypeVar

import lmdb

UNSET = object()

KeyType = str
ValType = TypeVar("ValType")


class Bigdict(MutableMapping, Generic[ValType]):
    """
    In the target use cases, writing and reading are well separated.
    Usually one creates a database and writes data into it.
    Once that's done, it's no longer appended to or revised; it's just
    used for reading.

    You may need to use extra caution in other usage patterns.

    If reading and writing are mixed, calling `commit` after writing and before reading
    should eliminate most issues. There is no need to call `commit` between consecutive writings
    or consecutive readings, or after reading and before writing.
    
    Across multiple threads in one process, there can be multiple readers but at most one writer at any time.
    If you want recent writing to be visible to readers (in the same or other threads), call `commit` after writing.
    If multiple threads write, call `commit` after a writing "streak" in one thread, and do not have overlapping
    writing streaks in threads.

    Follow these same rules in multiprocessing.

    If you write a large number of entries, using `update` with large batches is much more efficient than
    individual writes via `__setitem__`.
    """

    @classmethod
    def new(
        cls,
        path: str = None,
        *,
        shard_level: int = 0,
        **kwargs,
    ):
        """
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
        """
        assert shard_level in (0, 1, 8, 16, 32, 64, 128, 256)
        info = {
            "storage_version": 2,
            # `storage_version = 1` is introduced in release 0.2.0.
            # It was missing before that, and treated as 0.
            # Version 0 used a RocksDB backend;
            # version 1+ uses a LMDB backend.
            # `storage_version = 2` is introduced in release 0.2.9 in light of
            # a bug in version 1.
            "shard_level": shard_level,
            "key_pickle_protocol": 5,  # Added in 0.2.7. Record this to ensure consistency between insert and query times.
        }

        if path is None:
            path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        path = os.path.abspath(path)
        assert not os.path.isdir(path)
        os.makedirs(path)

        json.dump(info, open(os.path.join(path, "info.json"), "w"))
        z = cls(path, readonly=False, **kwargs)
        return z

    def __init__(
        self,
        path: str,
        *,
        map_size_mb: int = 64,
        readonly: bool = True,
    ):
        """
        Parameters
        ----------
        map_size_mb
            Max size of the database file for one shard.

            On Windows and possibly Mac, the database file (e.g., in `self.path / 'db' / '0' )
            size is set to be equal to ``map_size_mb`` upfront,
            hence you should not set an unnecessarily large ``map_size_mb``.
            On Linux, the database file size grows as needed by the actual data, hence setting a generously
            large ``map_size_mb`` is probably harmless.

            https://groups.google.com/g/caffe-users/c/0RKsTTYRGpQ?pli=1
            https://openldap.org/lists/openldap-technical/201511/msg00101.html
            https://openldap.org/lists/openldap-technical/201511/msg00107.html

            Experiments showed that ``map_size_mb`` is not an intrinsic attribute of the database files.
            (I did not read about this in documentation, nor do I understand how memory-mapping works.)
            You are free to choose a ``map_size_mb`` value unrelated to the value used when creating
            the database files, as long as the value is large enough for your application.
        """
        self.path = os.path.abspath(path)

        self.info = json.load(open(os.path.join(path, "info.json"), "r"))

        self._storage_version = self.info.get("storage_version", 0)
        if self._storage_version == 0:
            raise RuntimeError(
                "Support for RocksDB storage is removed in version 0.2.8. Please use Bigdict <= 0.2.7 to migrate this old dataset to the new format."
            )
            # This turned from warning to error in version 0.2.8 because installing rocksdb had issues.

        self._key_pickle_protocol = self.info.get("key_pickle_protocol", 5)
        # This value is in `self.info` starting with 0.2.7.

        self._shard_level = self.info.get("shard_level", 0)
        # DO NOT EVER manually modify ``self._storage_version`` and ``self._shard_level``.

        if self._storage_version == 1 and self._shard_level > 1:
            # "storage version 1" has a bug when "shard level > 1" so that persisted datasets
            # can not be read back in reliably.
            raise RuntimeError(
                "Storage version 1 is no longer supported. Please create new datasets to use a newer storage format."
            )

        self._map_size_mb = map_size_mb
        self._readonly = readonly
        self._dbs = {}
        self._transactions = {}

    @property
    def readonly(self):
        return self._readonly

    @property
    def map_size_mb(self):
        return self._map_size_mb

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.path}')"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def _reduce_rebuild(cls, path, map_size_mb, readonly):
        return cls(path, map_size_mb=map_size_mb, readonly=readonly)

    def __reduce__(self):
        return (
            self._reduce_rebuild,
            (type(self), self.path, self.map_size_mb, self.readonly)
        )

    def __del__(self):
        # Although this tries to flush unsaved changes, this is not totally reliable.
        # It is recommended to explicitly `flush` before you leave
        # after you make any writes, including updates to `info`.
        if getattr(self, "info", None) is not None:  # otherwise `destroy` has been called
            try:
                self.flush()
            except FileNotFoundError as e:
                if '/info.json' in str(e):
                    # Could not find the 'info' file or folder;
                    # could happen if this db has been "destroyed" by another client
                    pass
                else:
                    raise
            except NameError as e:
                if str(e) == "name 'open' is not defined":
                    # Could happen during interpreter shutdown.
                    pass
                else:
                    raise

            for x in self._dbs.values():
                x.close()
            self._dbs = {}

    def _shards(self) -> list[str]:
        # Return existing shards.
        # The shards permitted by `self._shard_level` may not all be existing (yet).
        if self._shard_level <= 1:
            return ["0"]

        files = []
        try:
            for f in os.listdir(os.path.join(self.path, "db")):
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
        sv = self._storage_version
        sl = self._shard_level
        if sv < 1 or sl <= 1:
            return "0"
        if sv == 2:
            if len(key) == 0:  # TODO: should we allow empty key value?
                return "0"
            base = blake2b(key, digest_size=1).digest()[0]  # 1 byte, used as int
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
            return str(base)
        raise ValueError(f"storage-version {sv}")

    def _env(self, shard: str, *, readonly=None, **config):
        map_size = self.map_size_mb * 1048576  # 1048576 is 2**20, or 1 MB
        if readonly:
            db = lmdb.Environment(
                os.path.join(self.path, "db", shard),
                create=False,
                readonly=True,
                subdir=True,
                readahead=False,
                map_size=map_size,
                **config,
            )
        else:
            os.makedirs(os.path.join(self.path, "db", shard), exist_ok=True)
            db = lmdb.Environment(
                os.path.join(self.path, "db", shard),
                readonly=False,
                subdir=True,
                readahead=False,
                map_size=map_size,
                writemap=True,
                map_async=True,
                **config,
            )
        return db

    def _db(self, shard: str = "0"):
        db = self._dbs.get(shard, None)
        if db is None:
            db = self._env(shard, readonly=self.readonly)
            self._dbs[shard] = db
        return db

    def _transaction(self, shard: str = "0"):
        if shard not in self._transactions:
            txn = lmdb.Transaction(self._db(shard), write=not self.readonly)
            txn.__enter__()  # this does nothing as of `lmdb` version 1.4.1.
            self._transactions[shard] = txn
        return self._transactions[shard]

    def commit(self):
        """
        Commit and close all pending transactions.

        :meth:`flush` is this ``commit`` plus saving the info file.
        If you know ``self.info`` has not changed and the overhead of saving
        the info file is significant in your use case (because for some reason
        you need to commit writes frequently), you can call ``commit`` instead
        of ``flush``.
        """
        for x in self._transactions.values():
            x.commit()
        self._transactions = {}

    def encode_key(self, k: KeyType) -> bytes:
        """
        The key should be a str.

        If a subclass uses bytes, then it should
        customize :meth:`encode_key` and :meth:`decode_key` to skip encoding
        and decoding.

        If a subclass uses neither str nor bytes, then it needs to
        provide its own stable and portable way to convert the key to bytes
        in its custom :meth:`encode_key` and :meth:`decode_key`.
        """
        # https://death.andgravity.com/stable-hashing#fnref-1
        return k.encode("utf-8")

    def decode_key(self, k: bytes) -> KeyType:
        return k.decode("utf-8")

    def encode_value(self, v: ValType) -> bytes:
        """
        As a general principle, do not persist pickled custom class objects.
        If ``v`` is not of a "native" Python class like str, dict, etc.,
        subclass should customize this method to convert ``v`` to a native type
        before pickling (or convert to bytes in a whole diff way w/o pickling).
        Correspnoding customization should happen in :meth:`decode_value`.
        """
        return pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)

    def decode_value(self, v: bytes) -> ValType:
        return pickle.loads(v)

    def __setitem__(self, key: KeyType, value: ValType):
        key = self.encode_key(key)
        shard = self._shard(key)
        value = self.encode_value(value)
        self._transaction(shard).put(key, value)

    def __getitem__(self, key: KeyType) -> ValType:
        k = self.encode_key(key)
        shard = self._shard(k)

        try:
            v = self._transaction(shard).get(k)
        except lmdb.PageNotFoundError:
            raise KeyError(key)
        # `v` can't be `None` as a valid return from the db,
        # because all values are bytes.
        if v is None:
            raise KeyError(key)
        return self.decode_value(v)

    def __delitem__(self, key: KeyType) -> None:
        k = self.encode_key(key)
        shard = self._shard(k)
        z = self._transaction(shard).delete(k)
        if not z:
            raise KeyError(key)

    def pop(self, key: KeyType, default=UNSET) -> ValType:
        k = self.encode_key(key)
        shard = self._shard(k)
        v = self._transaction(shard).pop(k)
        if v is None:
            if default is UNSET:
                raise KeyError(key)
            return default
        return self.decode_value(v)

    def setdefault(self, key: KeyType, value: ValType) -> ValType:
        try:
            return self[key]
        except KeyError:
            self[key] = value
            return value

    def update(self, data: Iterable[tuple[KeyType, ValType]] | Mapping[KeyType, ValType], /) -> None:
        sharddata = defaultdict(list)
        encode_key = self.encode_key
        encode_val = self.encode_value
        shard = self._shard
        if isinstance(data, Mapping):
            for k, v in data.items():
                kk = encode_key(k)
                vv = encode_val(v)
                s = shard(kk)
                sharddata[s].append((kk, vv))

        for sh, data in sharddata.items():
            with self._transaction(sh).cursor() as cursor:
                cursor.putmulti(data)

    def get(self, key: KeyType, default=None) -> ValType:
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self) -> Iterator[KeyType]:
        self.commit()
        decoder = self.decode_key
        for shard in self._shards():
            with self._db(shard).begin() as txn:
                for k in txn.cursor().iternext(keys=True, values=False):
                    yield decoder(k)

    def values(self) -> Iterator[ValType]:
        self.commit()
        decoder = self.decode_value
        for shard in self._shards():
            with self._db(shard).begin() as txn:
                for v in txn.cursor().iternext(keys=False, values=True):
                    yield decoder(v)

    def __iter__(self) -> Iterator[KeyType]:
        return self.keys()

    def items(self) -> Iterator[tuple[KeyType, ValType]]:
        self.commit()
        decode_key = self.decode_key
        decode_val = self.decode_value
        for shard in self._shards():
            with self._db(shard).begin() as txn:
                for key, value in txn.cursor().iternext(keys=True, values=True):
                    yield decode_key(key), decode_val(value)

    def __contains__(self, key: KeyType) -> bool:
        try:
            _ = self.__getitem__(key)
            return True
        except KeyError:
            return False

    def __len__(self) -> int:
        self.commit()
        n = 0
        for shard in self._shards():
            stat = self._db(shard).stat()
            n += stat["entries"]
        return n

    def __bool__(self) -> bool:
        self.commit()
        for shard in self._shards():
            stat = self._db(shard).stat()
            if stat["entries"]:
                return True
        return False

    def flush(self) -> None:
        """
        ``flush`` commits all writes (set/update/delete), and saves ``self.info``.
        Before ``flush`` is called, recent writes may not be available to reading.

        Do not call this after every write; instead, call this after a write "session",
        before you'done writing or you need to read.
        """
        self.commit()
        json.dump(self.info, open(os.path.join(self.path, "info.json"), "w"))

    def destroy(self) -> None:
        """
        After ``destroy``, disk data is erased and the object is no longer usable.
        """
        for x in self._transactions.values():
            x.abort()
        self._transactions = {}
        for x in self._dbs.values():
            x.close()
        self._dbs = {}
        shutil.rmtree(self.path, ignore_errors=True)
        try:
            delattr(self, "info")
        except AttributeError:
            pass

    def compact(self) -> None:
        """
        Perform a "copy with compaction" on the dataset.
        If successful, the older data files will be replaced by new ones.
        If unsuccessful, the first failing shard will be left unchanged, and the exception is raised.

        This could reduce file size considerably if

            - the dataset is huge
            - you have conducted a lot of writing since the last call to ``compact``

        This is an expensive operation. Don't do this often. You may want to do this
        at the end of a long writing session, and just before you are about to transfer
        the dataset files to another storage media.
        """
        # It is likely that after calling this method, the current object is no longer usable.
        # This is not carefully considered yet.
        self.flush()
        for x in self._dbs.values():
            x.close()
        self._dbs = {}
        size_old = 0  # bytes
        size_new = 0  # bytes

        def get_folder_size(folder):
            s = 0
            for file in os.scandir(folder):
                s += os.path.getsize(file)
            return s

        for shard in self._shards():
            shard_new = shard + "-new"
            path_new = os.path.join(self.path, "db", shard_new)

            db = self._env(shard)
            os.mkdir(path_new)
            db.copy(path_new, compact=True)

            try:
                db_new = self._env(shard_new, readonly=True)
            except Exception:
                shutil.rmtree(path_new)
                db.close()
                raise
            else:
                n = db.stat()["entries"]
                n_new = db_new.stat()["entries"]
                db.close()
                db_new.close()
                if n_new == n:
                    path = os.path.join(self.path, "db", shard)
                    size_old += get_folder_size(path)
                    shutil.rmtree(path)
                    os.rename(path_new, path)
                    size_new += get_folder_size(path)
                else:
                    shutil.rmtree(path_new)
                    raise RuntimeError(
                        f'expecting {n} entries but got {n_new} for shard "{shard}"'
                    )

        mb = 1048576  # 2**20
        size_old /= mb
        size_new /= mb

        print(
            f"Finished compressing LMDB dataset at '{self.path}' from {size_old:.2f} MB to {size_new:.2f} MB."
        )
