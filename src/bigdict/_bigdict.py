from __future__ import annotations

import itertools
import json
import os
import os.path
import pickle
import shutil
import tempfile
import uuid
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from hashlib import blake2b
from typing import Generic, TypeVar

import lmdb

UNSET = object()

KeyType = str
ValType = TypeVar("ValType")


ReadonlyError = lmdb.ReadonlyError


# TODO: `Generic[KeyType, ValType]`
# seems to require a newer Python version.
class Bigdict(MutableMapping, Generic[ValType]):
    """
    In many use cases, writing and reading are well separated.
    Usually one creates a database and writes data into it.
    Once that's done, it's no longer appended to or revised; it's just
    used for reading.

    If you mix reading and writing operations, see doc on the method :meth:`commit`.

    For precautions in threading and multiprocessing, see doc on :meth:`commit`.

    At the end of a writing session, call :meth:`flush`.
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

            This is not an intrinsic attribute of the database file(s).
            It seems that you are free to use different values of this in different readers
            of the same DB. But, to be worry free, it may be a good idea to use the same value.
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

        self._write_commit_interval = 1_000_000
        # You may change this value any time.

        self._num_pending_writes = 0
        # This is for internal use. Do not modify its value.

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
    def _reduce_rebuild(cls, path, map_size_mb):
        return cls(path, map_size_mb=map_size_mb, readonly=True)

    def __reduce__(self):
        # Probably all the uses for pickling is to send the object to another process.
        # Always send a readonly object.
        # If you need a read/write object, just pass the essential arguments and create an object
        # "manually".
        return (
            self._reduce_rebuild,
            (type(self), self.path, self.map_size_mb),
        )

    def _close(self):
        if not hasattr(self, "info"):
            return  # `destroy` has been called
        if self.readonly:
            return

        try:
            self.flush()
        except FileNotFoundError as e:
            if "/info.json" in str(e):
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

    def __del__(self):
        # If not using context manager, then
        # this tries to flush unsaved changes, but this is not totally reliable.
        # It is recommended to explicitly `flush` before you leave
        # after you make any writes, including updates to `info`.
        self._close()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self._close()

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

    def _track_write(self, n: int):
        if n < 1:
            return
        self._num_pending_writes += n
        if self._num_pending_writes >= self._write_commit_interval:
            self.commit()

    def commit(self):
        """
        Commit and close all pending transactions.

        :meth:`flush` is this ``commit`` plus saving the info file.
        If you know ``self.info`` has not changed and the overhead of saving
        the info file is significant in your use case (because for some reason
        you need to commit writes frequently), you can call ``commit`` instead
        of ``flush``.

        This is meaningful and allowed only when `self.readonly` is `False`.

        This is called automatically once a certain number of write operations
        (see `self._write_commit_interval`) have been conducted.
        However, in some situations you want to call this explicitly.

        In addition to automatic invocations at specified write intervals,
        this is also called when exiting context manager or when the object is garbage collected.
        However, to be explicit, it is recommended to call `commit` or `flush`
        at the end of a write session.

        If you use a single read/write object and interleave reading/writing operations,
        there is no need to call `commit` (so far as I have observed in tests). In other words,
        recent writings appear to be visible to reading, even though the writings do not seem
        to have been "committed".

        If you use multiple `Bigdict` objects to operate on the same underlying DB (and files),
        you need to use some cautions. (All discussions here about "multiple Bigdict objects"
        are in this sense. If they represent different DBs, they do not interfere with each other.)
        The core reason is related to the use of "transactions" in this class.

        In "readonly" mode, each read method opens a readonly transaction and closes it at the end
        of the method. However, note the methods :meth:`keys`, :meth:`values`, :meth:`items`
        are generators, hence the transaction's lifetime overlaps with other operations.

        In "read/write" mode, a read/write transaction remains alive across method calls; both read
        and write methods use this transaction. This makes writes immediately visible to subsequent reads
        regardless of commit or not (so far it seems to me). The transaction is closed by `commit`.
        Note that `commit` is automatically called at writing intervals, is also called
        at the beginning of several read methods (e.g., :method:`__len__`, :meth:`keys`, :meth:`values`,
        :meth:`items`), and may be called by the user anytime.

        If you use a read/write object to do reading only (which is NOT recommended), there is still
        a read/write transaction lurking there.

        If you use multiple `Bigdict` objects in one thread, or multiple threads, or multiple processes,
        remember this:

            There must be at most one read/write transaction living at any moment.

        Again, the method `commit` commits and **closes** the transaction, giving other objects a chance
        to start a new read/write transaction.

        Two read/write transactions living at the same time tend to hang the program. (This is the case
        even if you use a read/write object to do reading only---it still uses a read/write transaction.)

        After writing, if you want another object to read the changes right away, you may need to explicitly call
        :meth:`commit` on the first object.

        A read/write transaction can NOT be used in two threads. If you pass a read/write Bigdict to another thread,
        you need to call :meth:`commit` in the two threads (on the same object) at carefully coordinated times so that
        method calls (be it reading or writing) in the two threads do not use the same transaction. Indeed,
        to save yourself the trouble, do NOT pass a read/write object to another thread.

        In a long writing session (meaning inserting a large number of entries), the design of using a living transaction
        without too frequent commits has very significant performance benefits.
        """
        if self.readonly:
            raise ReadonlyError("commit: Permission denied")

        if self._num_pending_writes > 0:
            for x in self._transactions.values():
                x.commit()
            self._num_pending_writes = 0

        # If `self._num_pending_writes == 0`, there can still be
        # transactions created by `__getitem__`, but they did not perform
        # any write.
        self._transactions = {}
        # if not self.readonly:
        #     for db in self._dbs.values():
        #         db.sync()

    def flush(self) -> None:
        """
        ``flush`` commits all writes (set/update/delete), and saves ``self.info``.

        Do not call this after every write; instead, call this after a writing "session"
        to be sure all updates (including to `self.info`) are persisted.
        """
        self.commit()
        json.dump(self.info, open(os.path.join(self.path, "info.json"), "w"))

    def encode_key(self, k: KeyType) -> bytes:
        """
        The key should be a str.

        If a subclass uses bytes, it may want to
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
        Corresponding customization should happen in :meth:`decode_value`.

        If a subclass uses bytes values, it may want to customize :meth:`encode_value` and
        :meth:`decode_value` to skip encoding and decoding.
        """
        return pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)

    def decode_value(self, v: bytes) -> ValType:
        return pickle.loads(v)

    def __setitem__(self, key: KeyType, value: ValType):
        key = self.encode_key(key)
        shard = self._shard(key)
        value = self.encode_value(value)
        self._transaction(shard).put(key, value)
        # This raises ReadonlyError if `self.readonly` is True.
        self._track_write(1)

    def __getitem__(self, key: KeyType) -> ValType:
        k = self.encode_key(key)
        shard = self._shard(k)

        try:
            if self.readonly:
                with self._db(shard).begin() as txn:
                    v = txn.get(k)
                    # Using a new transaction on each read.
                    # If other objects have written, as long as they have committed,
                    # the changes will be visible to this read.
            else:
                v = self._transaction(shard).get(k)
                # In a read-write env, this will use the existing transaction,
                # which may have just written but haven't committed;
                # those writes will be visible here.
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
        # This raises ReadonlyError if `self.readonly` is True.
        if not z:
            raise KeyError(key)
        self._track_write(1)

    def pop(self, key: KeyType, default=UNSET) -> ValType:
        if self.readonly:
            raise ReadonlyError("pop: Permission denied")

        k = self.encode_key(key)
        shard = self._shard(k)
        v = self._transaction(shard).pop(k)
        if v is None:
            if default is UNSET:
                raise KeyError(key)
            return default
        self._track_write(1)
        return self.decode_value(v)

    def setdefault(self, key: KeyType, value: ValType) -> ValType:
        if self.readonly:
            raise ReadonlyError("setdefault: Permission denied")

        try:
            return self[key]
        except KeyError:
            self.__setitem__(key, value)
            return value

    def update(
        self,
        data: Iterable[tuple[KeyType, ValType]] | Mapping[KeyType, ValType] = (),
        /,
        **kwargs,
    ) -> None:
        """
        If you write a large number of entries, using :meth:`update` with large batches can have considerable speed gains
        compared to using :meth:`__setitem__` to add entries one by one.
        """

        if self.readonly:
            raise ReadonlyError("update: Permission denied")

        sharddata = defaultdict(list)
        encode_key = self.encode_key
        encode_val = self.encode_value
        shard = self._shard
        if isinstance(data, Mapping):
            other = data.items()
        else:
            other = data
        for k, v in itertools.chain(other, kwargs.items()):
            kk = encode_key(k)
            vv = encode_val(v)
            s = shard(kk)
            sharddata[s].append((kk, vv))

        n = 0
        for sh, values in sharddata.items():
            with self._transaction(sh).cursor() as cursor:
                cursor.putmulti(values)
                n += len(values)
        self._track_write(n)

    def get(self, key: KeyType, default=None) -> ValType:
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def keys(self) -> Iterator[KeyType]:
        if not self.readonly:
            self.commit()
        decoder = self.decode_key
        for shard in self._shards():
            with self._db(shard).begin() as txn:
                for k in txn.cursor().iternext(keys=True, values=False):
                    yield decoder(k)

    def values(self) -> Iterator[ValType]:
        if not self.readonly:
            self.commit()
        decoder = self.decode_value
        for shard in self._shards():
            with self._db(shard).begin() as txn:
                for v in txn.cursor().iternext(keys=False, values=True):
                    yield decoder(v)

    def __iter__(self) -> Iterator[KeyType]:
        return self.keys()

    def items(self) -> Iterator[tuple[KeyType, ValType]]:
        if not self.readonly:
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
        if not self.readonly:
            self.commit()
        # If there are recent writes in the same object, they may not be counted
        # w/o commit.
        # TODO:
        # we could track count changes in a writer w/o `commit`.

        n = 0
        for shard in self._shards():
            stat = self._db(shard).stat()
            n += stat["entries"]
        return n

    def __bool__(self) -> bool:
        if not self.readonly:
            self.commit()
        for shard in self._shards():
            stat = self._db(shard).stat()
            if stat["entries"]:
                return True
        return False

    def destroy(self) -> None:
        """
        After ``destroy``, disk data is erased and the object is no longer usable.
        """
        if self.readonly:
            raise ReadonlyError("destroy: Permission denied")

        for x in self._transactions.values():
            x.abort()
        self._transactions = {}
        self._num_pending_writes = 0
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

        After successful completion of this method, the current object
        (now pointing to a new, compact version) can continue to be used.
        """
        if self.readonly:
            raise ReadonlyError("compact: Permission denied")

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
