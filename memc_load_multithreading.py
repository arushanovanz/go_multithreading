#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import time
from optparse import OptionParser
import appsinstalled_pb2
import memcache
import threading
import queue
from concurrent.futures import ThreadPoolExecutor

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class MemcachedConnectionPool:
    _instances = {}
    _lock = threading.Lock()

    @classmethod
    def get_client(cls, memc_addr):
        with cls._lock:
            if memc_addr not in cls._instances:
                cls._instances[memc_addr] = memcache.Client([memc_addr])
            return cls._instances[memc_addr]


def dot_rename(path):
    head, fn = os.path.split(path)
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = MemcachedConnectionPool.get_client(memc_addr)
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line = line.decode('utf-8').strip()
    line_parts = line.split("\t")
    if len(line_parts) < 5:
        return None
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return None
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isdigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
        return None
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def line_worker(options, line_queue, stats):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    while True:
        try:
            line = line_queue.get(timeout=1)
            if line is None:
                line_queue.task_done()
                break

            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                with stats['lock']:
                    stats['errors'] += 1
                line_queue.task_done()
                continue

            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                with stats['lock']:
                    stats['errors'] += 1
                logging.error("Unknown device type: %s" % appsinstalled.dev_type)
                line_queue.task_done()
                continue

            ok = insert_appsinstalled(memc_addr, appsinstalled, options.dry)
            with stats['lock']:
                if ok:
                    stats['processed'] += 1
                else:
                    stats['errors'] += 1

            line_queue.task_done()
        except queue.Empty:
            break


def process_file(options, filename):
    line_queue = queue.Queue(maxsize=10000)
    stats = {
        'processed': 0,
        'errors': 0,
        'lock': threading.Lock()
    }

    with ThreadPoolExecutor(max_workers=options.workers) as executor:
        futures = []
        for _ in range(options.workers):
            futures.append(executor.submit(line_worker, options, line_queue, stats))

        try:
            with gzip.open(filename, 'rb') as fd:
                for line in fd:
                    line = line.strip()
                    if line:
                        line_queue.put(line)
        except Exception as e:
            logging.error("Error reading file %s: %s" % (filename, e))

        for _ in range(options.workers):
            line_queue.put(None)

        for future in futures:
            future.result()

    if not stats['processed']:
        dot_rename(filename)
        return True

    err_rate = float(stats['errors']) / stats['processed']
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successful load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))

    dot_rename(filename)
    return True


def main(options):
    for fn in glob.iglob(options.pattern):
        process_file(options, fn)

def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked

if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--workers", type="int", default=8, help="Number of worker threads")
    (opts, args) = op.parse_args()

    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        start_time = time.time()
        main(opts)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)