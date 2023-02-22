# Changelog

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
