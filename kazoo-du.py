#!/usr/bin/env python

from __future__ import print_function

import argparse
from Queue import Empty, Queue
from threading import Event, Thread

from kazoo.client import KazooClient


def get_params():
    parser = argparse.ArgumentParser()
    parser.add_argument("hosts", type=str)
    parser.add_argument("path", type=str)

    return parser.parse_args()


class Request(object):
    __slots__ = ("path", "result")

    def __init__(self, path, result):
        self.path, self.result = path, result

    def get(self):
        return self.result.get()


def main(hosts, path):
    exists = Queue()
    children = Queue()
    counters = {"child": 1, "total": 0}
    done = Event()
    zk = KazooClient(hosts)
    zk.start()

    exists.put(Request(path, zk.exists_async(path)))

    def exists_worker(zk, counters):
        while done.isSet() is False:
            try:
                e = exists.get()
            except Empty as ex:
                continue

            stat = e.get()
            counters["total"] += stat.dataLength
            counters["child"] += stat.numChildren
            if stat.numChildren > 0:
                children.put(Request(e.path, zk.get_children_async(e.path)))
            counters["child"] -= 1

            if counters["child"] == 0:
                done.set()

    def children_worker(zk):
        while done.isSet() is False:
            try:
                c = children.get(block=False, timeout=1)
            except Empty as ex:
                continue

            ppath = c.path if c.path == "/" else c.path + "/"
            for child in c.get():
                cpath = "%s%s" % (ppath, child)
                exists.put(Request(cpath, zk.exists_async(cpath)))

    texists = Thread(target=exists_worker, args=(zk, counters))
    texists.start()

    tchildren = Thread(target=children_worker, args=(zk,))
    tchildren.start()

    done.wait()

    print("Total = %d" % (counters["total"]))

    zk.stop()

    texists.join()
    tchildren.join()


if __name__ == "__main__":
    params = get_params()
    main(params.hosts, params.path)
