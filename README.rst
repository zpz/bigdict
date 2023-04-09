bigdict
=======

``bigdict`` implements a persisted, out-of-memory dict for Python.

The usage API strives to be as close as possible to the built-in ``dict``, with a small number of extras.

The "engine", or "back-end", is currently ``lmdb``. However, ``lmdb`` is not the point; the point is rather a "persisted, out-of-memory dict". Criteria used in choosing the engine:

- It must be *embedded*, as opposed to client/server.
- It must be a mature project with a reliable Python binding.

In addition,

- It must use more files for storage as the data size grows, hence keeping file sizes modest,
  as opposed to a single file growing in size.

If the chosen backend does not meet this requirement, it's the responsibility of ``bigdict`` to implement that.

Installation::


    $ pip install bigdict

