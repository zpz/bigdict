# bigdict

`bigdict` implements a persisted, out-of-memory dict for Python.

Tested with Python 3.7, 3.8.

The usage API strives to be as close as possible to the built-in `dict`, with a small number of extras.

The "engine", or "back-end", is currently `rocksdb`. However, `rocksdb` is not the point; the point is rather a "persisted, out-of-memory dict".

Installation:

```
$ pip install bigdict
```


Possible enhancements in the future:

- Support remote storage.
- Implement a cache backed by `bigdict`.
- A more performant engine to replace `rocksdb`.
- Make updates to the package `python-rocksdb`, which is not very active as of now.
