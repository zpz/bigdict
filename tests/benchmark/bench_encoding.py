from uuid import uuid4
from time import perf_counter
from pickle import dumps, loads


def bench_encode(data, n):
    t0 = perf_counter()
    for _ in range(n):
        for x in data:
            y = x.encode('utf-8')
            z = y.decode('utf-8')
    t1 = perf_counter()
    print('encode/decode took', t1 - t0, 'seconds')


def bench_pickle(data, n):
    t0 = perf_counter()
    for _ in range(n):
        for x in data:
            y = dumps(x)
            z = loads(y)
    t1 = perf_counter()
    print('pickle/unpickle took', t1 - t0, 'seconds')


def main():
    data = [str(uuid4()) for _ in range(1000000)]
    bench_encode(data, 100)
    print()
    bench_pickle(data, 100)


if __name__ == '__main__':
    main()




