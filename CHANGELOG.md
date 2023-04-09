# Changelog

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
