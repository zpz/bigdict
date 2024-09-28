# This benchmark compares the Bigdict read/write speed against pickle/unpickle.

from multiprocessing import get_context
from time import perf_counter
import threading

from bigdict import Bigdict
from faker import Faker
from zpz.timer import timed



def make_data(n=100):
    fake = Faker()
    data = {}
    for _ in range(n):
        key = fake.text(80)
        val = fake.profile()
        data[key] = val
    return data


@timed()
def write_bd(bd, data):
    for k, v in data.items():
        bd[k] = v
    bd.flush()



def mp_read(bd, keys):
    for key in keys:
        _ = bd.get(key)


@timed()
def write_read(data, keys):
    bd = Bigdict.new()
    write_bd(bd, data)

    ctx = get_context('spawn')
    p = ctx.Process(
        target=mp_read,
        args=(bd, keys),
    )
    p.start()
    p.join()

    bd.destroy()


def receive_data(q, k):
    n = 0
    while True:
        z = q.get()
        if z is None:
            break
        n += 1
    if n != k:
        print('expect', k, 'got', n)


def feed_data(q, data):
    for z in data.items():
        q.put(z)
    q.put(None)


@timed()
def mp_queue(data):
    ctx = get_context('spawn')
    q = ctx.Queue(maxsize=1000)

    feeder = threading.Thread(
        target=feed_data,
        args=(q, data)
    )
    receiver = ctx.Process(
        target=receive_data,
        args=(q, len(data)),
    )
    feeder.start()
    receiver.start()
    feeder.join()
    receiver.join()



def main():
    data = make_data(100000)
    keys = list(data.keys())

    print()
    write_read(data, keys)
    print()
    mp_queue(data)


if __name__ == '__main__':
    main()


# 2024/9/21, ran with 100000 data elements:

# $ python bench_pickle.py 

# Starting function `write_read`
# Starting function `write_bd`
# Finishing function `write_bd`
# Function `write_bd` took 1.7491 seconds to finish
# Finishing function `write_read`
# Function `write_read` took 3.2201 seconds to finish

# Starting function `mp_queue`
# Finishing function `mp_queue`
# Function `mp_queue` took 2.558 seconds to finish

