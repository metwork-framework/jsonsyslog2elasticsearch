#!/usr/bin/env python3

import ujson
import argparse
import elasticsearch
import functools
import elasticsearch.helpers
import importlib
from datetime import datetime, timezone
import time
import ciso8601
import signal
import socketserver
import queue
import pytz
import sys
import threading
import logging
from mflog import get_logger, set_config

DESCRIPTION = "syslog daemon which accept only UTF-8 JSON messages to send " \
    "them to an elasticsearch instance with an optional transformation"

LOG = get_logger("jsonsyslog2elasticsearch")
RUNNING = True
TO_SEND = []
CHUNK_SIZE = 20000
SYSLOG_THREAD = None
LOG_QUEUE_SIZE_EVERY = 5
DISCARDED = 0


def silent_elasticsearch_logger():
    logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)


def signal_handler(signum, frame):
    global RUNNING
    LOG.info("Signal: %i handled => let's stop", signum)
    RUNNING = False
    if SYSLOG_THREAD is not None:
        SYSLOG_THREAD.shutdown()


def process(line, transform_func, index_func):
    if line is None:
        return False
    tmp = line.strip().rstrip('\x00')
    if len(tmp) == 0:
        return False
    try:
        decoded = ujson.loads(tmp)
    except Exception:
        LOG.warning("can't decode the line: %s as JSON" % tmp)
        return False
    if not isinstance(decoded, dict):
        LOG.warning("the decoded line: %s must be a JSON dict" % tmp)
        return False
    index = index_func(decoded)
    transformed = transform_func(decoded)
    if transformed is None:
        return False
    if not isinstance(transformed, dict):
        LOG.warning("the transformed function must return a dict (or None)"
                    ", got: %s" % type(transformed))
        return False
    TO_SEND.append({
        "_op_type": "index",
        "_index": index,
        "_type": "_doc",
        "_source": transformed
    })
    return True


def commit(es, force=False):
    global TO_SEND
    if not force and len(TO_SEND) < CHUNK_SIZE:
        return False
    if len(TO_SEND) == 0:
        return False
    LOG.info("commiting %i line(s) to ES..." % len(TO_SEND))
    before = time.time()
    try:
        res = elasticsearch.helpers.bulk(es, TO_SEND, stats_only=False,
                                         chunk_size=CHUNK_SIZE,
                                         raise_on_error=False)
        if res[0] != len(TO_SEND):
            LOG.warning("can't post %i lines to ES" %
                        (len(TO_SEND) - res[0]))
            LOG.debug("raw output: %s" % res[1])
        else:
            LOG.info("commited %i line(s) to ES in %i ms" %
                     (res[0], int(1000.0 * (time.time() - before))))
    except Exception as e:
        LOG.warning("can't commit anything to ES: %s "
                    "(ES or network problem ?)", e)
    TO_SEND = []
    return True


def no_transform(dict_object):
    return dict_object


def _get_func(func_label, func_path):
    func_name = func_path.split('.')[-1]
    module_path = ".".join(func_path.split('.')[0:-1])
    if module_path == "":
        LOG.error("%s must follow 'pkg.function_name'" % func_label)
        sys.exit(1)
    if func_path.endswith(')'):
        LOG.error("%s must follow 'pkg.function_name'" % func_label)
        sys.exit(1)
    mod = importlib.import_module(module_path)
    return getattr(mod, func_name)


def get_transform_func(func_path):
    return _get_func("transform-func", func_path)


def get_index_func(func_path):
    return _get_func("transform-func", func_path)


def default_index_func(index_const_value, dict_object):
    if "@timestamp" in dict_object:
        ref = ciso8601.parse_datetime(dict_object["@timestamp"])
        ref_utc = ref.replace(tzinfo=pytz.utc) - ref.utcoffset()
    else:
        # utcnow() is deprecated
        try:
            ref_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        except Exception:
            ref_utc = datetime.utcnow()
    return ref_utc.strftime(index_const_value)


class SysLogUDPHandler(socketserver.BaseRequestHandler):

    queue = None
    queue_timeout = None

    def handle(self):
        global DISCARDED
        try:
            data = bytes.decode(self.request[0].strip(), encoding="utf-8")
        except Exception:
            LOG.warning("bad data received, can't decode as UTF-8")
            return
        try:
            self.queue.put(data, block=False)
        except queue.Full:
            DISCARDED = DISCARDED + 1


class SysLogUDPReceiverThread(threading.Thread):

    def __init__(self, syslog_host, syslog_port, queue, queue_timeout):
        threading.Thread.__init__(self, name="SysLogUDPReceiverThread")
        self.queue = queue
        self.queue_timeout = queue_timeout
        self.host = syslog_host
        self.port = syslog_port
        self.server = None

    def run(self):
        handler = SysLogUDPHandler
        handler.queue = self.queue
        handler.queue_timeout = self.queue_timeout
        self.server = socketserver.UDPServer((self.host, self.port), handler)
        LOG.info("Listening (UDP) to %s:%i..." % (self.host, self.port))
        self.server.serve_forever(poll_interval=0.5)
        LOG.info("Listening stopped")

    def shutdown(self):
        if self.server is not None:
            self.server.shutdown()


def main():
    global SYSLOG_THREAD, DISCARDED
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("--transform-func",
                        default="jsonsyslog2elasticsearch.no_transform",
                        help="python function to transform each json line")
    parser.add_argument("--debug",
                        action="store_true",
                        help="force debug mode")
    parser.add_argument("--syslog-port", type=int, default=5144,
                        help="UDP port to listen for syslog messages "
                        "(warning: the default value: 5144 is not the "
                        "default syslog port)")
    parser.add_argument("--syslog-host", type=str, default="0.0.0.0",
                        help="can be use to bind to a specific interface "
                        "(default: 0.0.0.0 => all)")
    parser.add_argument(
        "--internal-queue-size", type=int, default=10000,
        help="internal queue maximum size (in messages) between syslog "
        "receiver and elasticsearch upload (default: 10000)"
    )
    parser.add_argument(
        "--internal-queue-timeout", type=int, default=3,
        help="if the internal queue is full during this interval (in seconds "
        ", default to 3), then we will drop some messages "
    )
    parser.add_argument("--index-override-func",
                        default="no",
                        help="python function to override the ES_INDEX value")
    parser.add_argument("ES_HOST", help="ES hostname/ip")
    parser.add_argument("ES_PORT", help="ES port", type=int)
    parser.add_argument("ES_INDEX", help="ES index name", type=str)
    args = parser.parse_args()
    if args.debug:
        set_config(minimal_level="DEBUG")
    silent_elasticsearch_logger()
    transform_func = get_transform_func(args.transform_func)
    if args.index_override_func == "no":
        index_func = functools.partial(default_index_func, args.ES_INDEX)
    else:
        index_func = get_index_func(args.index_override_func)
    es = elasticsearch.Elasticsearch(hosts=[{
        "host": args.ES_HOST,
        "port": args.ES_PORT,
        "use_ssl": False
    }])
    signal.signal(signal.SIGTERM, signal_handler)
    q = queue.Queue(maxsize=args.internal_queue_size)
    SYSLOG_THREAD = SysLogUDPReceiverThread(args.syslog_host, args.syslog_port,
                                            q, args.internal_queue_timeout)
    SYSLOG_THREAD.start()
    last_queue_size_log = time.time()
    while RUNNING:
        now = time.time()
        if (now - last_queue_size_log) > LOG_QUEUE_SIZE_EVERY:
            size = q.qsize()
            if size > 0:
                LOG.info("Internal queue size: %i" % size)
            if DISCARDED > 0:
                LOG.warning(
                    "%i lines were discarded because of full queue during "
                    "last %i seconds" % (DISCARDED, LOG_QUEUE_SIZE_EVERY))
                DISCARDED = 0
            last_queue_size_log = now
        try:
            item = q.get(block=True, timeout=1)
        except Exception:
            commit(es, True)
            continue
        f = item.find('{')
        if f == -1:
            LOG.warning("can't find { char in read message: %s => ignoring",
                        item)
            continue
        json_string = item[f:]
        if process(json_string, transform_func, index_func):
            commit(es)


if __name__ == "__main__":
    main()
