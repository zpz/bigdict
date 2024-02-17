from time import perf_counter
import hashlib


def main():
    lines = [l.encode('utf8') for l in open('../../src/bigdict/_bigdict.py')]

    for hasher in (hashlib.md5, hashlib.sha1, hashlib.sha256, hashlib.sha3_256, hashlib.blake2b, hashlib.blake2s):
        t0 = perf_counter()
        for _ in range(10000):
            for line in lines:
                h = hasher(line).digest()
        t1 = perf_counter()
        print(hasher.__name__, t1 - t0)


        t0 = perf_counter()
        for _ in range(10000):
            for line in lines:
                h = hasher(line, usedforsecurity=False).digest()
        t1 = perf_counter()
        print(hasher.__name__, 'not secure', t1 - t0)


    for hasher in (hashlib.blake2b, hashlib.blake2s):
        t0 = perf_counter()
        for _ in range(10000):
            for line in lines:
                h = hasher(line, digest_size=1).digest()
        t1 = perf_counter()
        print(hasher.__name__, '1 byte', t1 - t0)


    for hasher in (hashlib.shake_128,):
        t0 = perf_counter()
        for _ in range(10000):
            for line in lines:
                h = hasher(line).digest(length=1)
        t1 = perf_counter()
        print(hasher.__name__, t1 - t0)


if __name__ == '__main__':
    main()


'''
Result on 2024/2/17:

openssl_md5 3.8302133440001853
openssl_md5 not secure 4.356458257000668
openssl_sha1 3.824289244999818
openssl_sha1 not secure 4.091215821999867
openssl_sha256 4.3671360179996555
openssl_sha256 not secure 4.59745625800042
openssl_sha3_256 5.528286671000387
openssl_sha3_256 not secure 5.781723396999951
blake2b 2.551422989000457
blake2b not secure 3.8203258849998747
blake2s 2.2960757780001586
blake2s not secure 3.590377449000698
blake2b 1 byte 3.0486211569996158
blake2s 1 byte 2.795929935999993
openssl_shake_128 5.9937924679998105
'''

