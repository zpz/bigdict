# write
| items |bigdict|lmdbm |
|------:|-------|------|
|     10| 0.0012|0.0152|
|    100| 0.0025|0.1543|
|   1000| 0.0121|1.7761|
|  10000| 0.1235|-     |
| 100000| 1.1683|-     |
|1000000|-      |-     |
# batch_write
| items |bigdict|lmdbm |
|------:|------:|------|
|     10| 0.0011|0.0024|
|    100| 0.0019|0.0046|
|   1000| 0.0087|0.0110|
|  10000| 0.0824|0.1029|
| 100000| 0.8644|0.9920|
|1000000| 8.6120|-     |
# read
| items |bigdict|lmdbm |
|------:|-------|------|
|     10| 0.0008|0.0004|
|    100| 0.0020|0.0030|
|   1000| 0.0108|0.0128|
|  10000| 0.1281|0.1257|
| 100000| 1.3534|1.2680|
|1000000|-      |-     |
# batch_read
| items |bigdict|lmdbm|
|------:|-------|-----|
|     10| 0.0009|-    |
|    100| 0.0017|-    |
|   1000| 0.0099|-    |
|  10000| 0.1025|-    |
| 100000| 1.0701|-    |
|1000000|-      |-    |
# combined
| items |bigdict|lmdbm |
|------:|------:|------|
|     10| 0.0127|0.1588|
|    100| 0.0128|0.1762|
|   1000| 0.0127|0.1679|
|  10000| 0.0131|0.2132|
| 100000| 0.0135|0.1630|
|1000000| 0.0138|-     |