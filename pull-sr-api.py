#! /usr/bin/env python
import requests
from threading import Thread, Event
from queue import Queue
import json
import logging
import time
import argparse

# Argument Parsing
parser = argparse.ArgumentParser()

parser.add_argument(
    "-n",
    "--num-of-threads",
    action="store",
    type=int,
    default=4,
    help="The number of threads to use, Speedrun.com has issues with anything more than 6"
)

parser.add_argument(
    "-u",
    "--url",
    action="store",
    type=str,
    default="https://www.speedrun.com/api/v1",
    help="API url to fetch from"
)

parser.add_argument(
    "-q",
    "--quiet",
    action="store_true",
    default=False,
    help="Silence output"
)

args = parser.parse_args()

if args.quiet:
    logging.basicConfig(level=logging.ERROR, format='[%(relativeCreated)6d]:%(threadName)s >> %(message)s')
else:
    logging.basicConfig(level=logging.INFO, format='[%(relativeCreated)6d]:%(threadName)s >> %(message)s')



url_root = args.url
url_queue = Queue()
api_results = Queue()
num_workers = args.num_of_threads
done_flag = Event()


def add_pages(url_q, done: Event):
    for i in range(0, 959000, 200):
        if done.is_set():
            url_q.mutex.acquire()
            url_q.queue.clear()
            url_q.all_tasks_done.notify_all()
            url_q.unfinished_tasks = 0
            url_q.mutex.release()
            return
        url_q.put({"offset": i, "length": 200})


def worker(url_q: Queue, api_q: Queue, done: Event):
    while True:
        logging.info(f"Fetching new page")
        if url_q.empty():
            return
        url_info = url_q.get()
        logging.info(
            f"Fetching Page: {url_root}/runs?max={url_info['length']}&offset={url_info['offset']}&orderby=submitted&direction=asc"
        )
        api_result = requests.get(
            f"{url_root}/runs?max={url_info['length']}&offset={url_info['offset']}&orderby=submitted&direction=asc"
        )
        while api_result.status_code != 200:
            logging.warning("Got non-200 status code, retrying in 20 seconds")
            time.sleep(20)
            logging.info("Retrying")
            api_result = requests.get(
                f"{url_root}/runs?max={url_info['length']}&offset={url_info['offset']}&orderby=submitted&direction=asc"
            )
        logging.info("Result: " + repr(api_result))
        decoded_results = api_result.json()
        if done.is_set():
            return
        if len(decoded_results['data']) == 0:
            logging.warning("No data found, exiting")
            done.set()
            logging.info("Queue cleared")
            return
        api_q.put(decoded_results)
        url_q.task_done()


def write_out():
    out = []
    while not api_results.empty():
        tmp = api_results.get()
        out.extend(tmp['data'])
        api_results.task_done()
    outfile = open('/Users/ennis/sr-dump.json', 'w+')
    json.dump(out, outfile)


generator = Thread(target=add_pages, args=(url_queue, done_flag))
generator.setDaemon(True)
generator.start()

for i in range(num_workers):
    daemon = Thread(target=worker, args=(url_queue, api_results, done_flag))
    daemon.setDaemon(True)
    daemon.start()

while not done_flag.is_set():
    time.sleep(1)

write_out()

logging.info("DONE")
