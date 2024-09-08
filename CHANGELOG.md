# Changelog


## [0.3.4] - in progress

- More cleanup in `Bigdict.flush`.
- New methods `as_readonly` and `as_readwrite`.


## [0.3.3] - 2024-06-13

- `get_buffer` commits in a read/write object if needed, so that user does not need to remember to commit after writes and before calling it.


## [0.3.2] - 2024-06-13

- Fixed/improved `__len__` and `__bool__`.
- New method `get_buffer`.
- New parameter `buffers` to methods `values` and `items`.


## [0.3.1] - 2024-06-12

- Removed context manager; use multiprocessing.util.Finalize instead.


## [0.3.0] - 2024-06-11

Significant refactor with simplifications, corrections, and enhancements.

- Complete redesign around the use of transactions. Do not use separate transactions for reading and writing.
- Added method `update`.
- Do not call `destroy` in `__del__`.
- Removed parameter `keep_files` to `Bigdict.new`.
- Added parameter `readonly` to `__init__`, default to `True`.


## [0.2.9] - 2024-02-17

- Changed Python requirement to 3.10.
- Changed KeyType to `str`; changed (or fixed) methods `encode_key`, `decode_key`, and `_shard`.
- Bumped "storage_version" to 2.


## [0.2.8] - 2023-08-13

- Made ``BigDict`` subclass ``MutableMapping`` and ``Generic``; improved type annotations.
- Remove support for ``rocksdb``.


## [0.2.7] - 2023-06-18

- New method ``compact``.
- Revised parameter handling regarding ``map_size``.
- Removed parameter ``read_only`` to ``__init__``.


## [0.2.6] - 2023-05-24

- Add support for sharding via the parameter ``shard_level`` to ``Bigdict.new``.
- Change default ``map_size`` to 64 MB.


## [0.2.5] - 2023-05-22

- Allow customizing ``lmdb.Environment`` config by additional args passed to ``__init__`` and ``new``.


## [0.2.4] - 2023-05-22

Revoked. A new parameter introduced is revised in 0.2.5.


## [0.2.3] - 2023-05-17

- Small bug fix.


## [0.2.2] - 2023-04-14

### Added

- Method `setdefault`
- Method `commit`
- Method `rollback`

### Enhancements

- Fix pickle protocol for ``encode_key``.


## [0.2.1] - [2023-04-09]

- Makes dependency on `rocksdb` optional.


## [0.2.0] - [2023-04-09]

Major refactor to move storage engine from RocksDB to LMDB.
While the reading of old datasets is supported for a while, API will evolve
(including some breaking changes) with the new storage engine.

### Removed

- class `DictView`
- method `clear`

### Added

- method `reload`


## [0.1.0] - [2023-04-07]

- Revert dependency from `lbry-rocksdb` back to `rocksdb` because the former does not provide
  build artifacts for python 3.10.
- Change version schema away from datetime; yank the previous releases; prep to move away from rocksdb in   upcoming releases.
- Change devel base image from `py3` to `py3-build`, which contains Rocksdb dependencies.

This release changes versioning scheme from datetime-based to numeric and prepare to refactor in the future.

This release 0.1.0 is the same as the previous 22.5.29.

There are difficulties in the ordering or even the visibility of the version 0.1.0 and the older ones 22.5.29, 21.4.11, and 21.1.5 on Github.
Since the older releases are not that important going forward, they are deleted. Their info remains in CHANGELOG.md.


## [22.5.29] - [2022-05-29]

- Changed dependency to `lbry-rocksdb`, which has more recent maintenance.
- Bumped Python requirement from 3.7 to 3.8, because that is the version this is developed and tested on,
  although the code does not use any recent language features.


## [21.4.11] - [2021-04-11]

- Changed dependency from `python-rocksdb` to `rocksdb`, which provides a built package in a wheel, greately simplifying build and installation.
- Bumped Python requirement from 3.6 to 3.7, because `rocksdb` provides wheels for 3.7-3.8 only.
- Enforce all `mypy` and `pylint` checks.


## [21.1.5] - [2021-01-05]

Initial release. Uploaded to Pypi.
