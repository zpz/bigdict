from time import sleep
from bigdict import Bigdict
import mpservice


def makedb(paths, name):
    db = Bigdict.new()
    db._db()
    paths[name] = db.path
    del db


def work(paths):
    for name in ('a', 'b', 'c', 'd', 'e'):
        print()
        print('making', name)
        makedb(paths, name)
        print('made', name)
        sleep(1)


def main():
    with mpservice.multiprocessing.Manager() as manager:
        paths = manager.dict()

        worker = mpservice.multiprocessing.Process(
            target=work,
            kwargs={'paths': paths},
        )
        worker.start()
        worker.join()


if __name__ == '__main__':
    main()
