# bigdict

`bigdict` implements a persisted, out-of-memory dict for Python.

Tested with Python 3.6, 3.7, 3.8, 3.9.

The usage API strives to be as close as possible to the built-in `dict`, with a small number of extras.

The "engine", or "back-end", is currently `rocksdb`. However, `rocksdb` is not the point; the point is rather a "persisted, out-of-memory dict".

Installation: `pip install bigdict`. You need to have `rocksdb` and a compilation toolchain installed while installing this. Please refer to [`Dockerfile-dev`](https://github.com/zpz/bigdict/blob/main/Dockerfile-dev) and [`Dockerfile-release`](https://github.com/zpz/bigdict/blob/main/Dockerfile-release).


Possible enhancements in the future:

- Support remote storage.
- Implement a cache backed by `bigdict`.
- A more performant engine to replace `rocksdb`.
- Make updates to the package `python-rocksdb`, which is not very active as of now.
- Provide a wheel that includes the `rocksdb` lib.
