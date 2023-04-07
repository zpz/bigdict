bigdict
=======

``bigdict`` implements a persisted, out-of-memory dict for Python.

The usage API strives to be as close as possible to the built-in ``dict``, with a small number of extras.

The "engine", or "back-end", is currently ``rocksdb``. However, ``rocksdb`` is not the point; the point is rather a "persisted, out-of-memory dict". Criteria used in choosing the engine:

- It must be *embedded*, as opposed to client/server.
- It must use more files for storage as the data size grows, hence keeping file sizes modest,
  as opposed to a single file growing in size. (This ruled out, e.g. SQLite and LMDB.)
- It must be a mature project with a reliable Python binding.


Installation::


    $ pip install bigdict

Possible enhancements in the future:

- Support remote storage.
- Implement a cache backed by ``bigdict``.
- A more performant engine to replace ``rocksdb``.
