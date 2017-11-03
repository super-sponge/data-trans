#!/usr/bin/env python
# -*- coding:utf-8 -*-

import datetime
import logging
import os
import argparse
import time
import multiprocessing
import random
import pyinotify
import commands
import json
import shutil

log_dir = "/tmp/log"
result_dir = "/tmp/result"
config_dir = "/tmp/config"


class EventHandler(pyinotify.ProcessEvent):
    def __init__(self, jobs):
        self.jobs = jobs
		
    def process_job(self, event):
        dt = datetime.datetime.now()
        dtstr = dt.strftime("%Y%m%d%H%M%S")

        log_name = os.path.join(log_dir, "%s_%s.log"%(event.name, dtstr))
        result_name = os.path.join(result_dir, "%s_%s.re"%(event.name, dtstr))

        report("add job [%s] %s %s %s"%(dtstr, event.name, log_name, result_name))

        self.jobs.put((event.pathname, log_name, result_name))
	

    def process_IN_CREATE(self, event):
        self.process_job(event)
		
    def process_IN_MOVED_TO(self, event):
        self.process_job(event)


def execute_job(datax_jobs, log_name, result_name):
    cmd = "python /usr/local/datax/bin/datax.py " + datax_jobs
    report("execute " + cmd)
    dtstart = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    status, output = commands.getstatusoutput(cmd)
    with open(log_name, 'w') as f:
        f.write(output)
    dtend = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    result = {}

    table_name = os.path.basename(datax_jobs).split('.')[0]

    result["targetConOrderTblName"] = table_name
    syncStartTimeStamp = dtstart
    syncEndTimeStamp = dtend
    syncAllDataCnt = 0
    syncFailureDataCnt = 0
    resultMsg = ""
    if status != 0:
        resultCode = 1
    else:
        resultCode = 0

    for line in output.split('\n'):
        if line.startswith("任务启动时刻"):
            syncStartTimeStamp = line.split(": ")[1].replace("-", "").replace(":", "").replace(" ", "")
        if line.startswith("任务结束时刻"):
            syncEndTimeStamp = line.split(": ")[1].replace("-", "").replace(":", "").replace(" ", "")
        if line.startswith("读出记录总数"):
            syncAllDataCnt = int(line.split(": ")[1])
        if line.startswith("读写失败总数"):
            syncFailureDataCnt = int(line.split(": ")[1])
        if line.startswith("com.alibaba.datax.common.exception.DataXException"):
            resultCode = 1
            resultMsg = line


    result["syncStartTimeStamp"] = syncStartTimeStamp
    result["syncEndTimeStamp"] = syncEndTimeStamp
    result["syncAllDataCnt"] = syncAllDataCnt
    result["syncFailureDataCnt"] = syncFailureDataCnt
    result["resultCode"] = resultCode
    result["resultMsg"] = resultMsg

    backconfig = os.path.basename(log_name).replace(".log", ".json")
    shutil.move(datax_jobs, os.path.join(config_dir, backconfig))
    with open(result_name, "w") as f:
        f.write(json.dumps(result))
    return resultCode


def report_results(nouse, results):
    while True:
        datax_cfg, state = results.get()
        report("%s state %s"%(datax_cfg, state))


def report(message, error=False):
    if error:
        logging.error(message)
    else:
        logging.info(message)


def handle_commandline():
    global log_dir, result_dir, config_dir
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--concurrency", type=int,
                        default=multiprocessing.cpu_count(),
                        help="specify the cocurrency"
                             "[default: %(default)d]")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="increase output verbosity")
    parser.add_argument("-l", "--log", default="/tmp/log",
                        help="log dir [default: %(default)s]")
    parser.add_argument("-r", "--result", default="/tmp/result",
                        help="result dir [default: %(default)s]")
    parser.add_argument("-b", "--backup", default="/tmp/config",
                        help="config backup dir [default: %(default)s]")
    parser.add_argument("source",
                        help="datax configuration dir [default: %(default)s]")

    args = parser.parse_args()
    log_dir = args.log
    result_dir = args.result
    config_dir = args.backup

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)

    log_init(log_dir)
    report("monitor_dir:[%s] log_dir:[%s] result_dir: [%s] config_dir:[%s]"%(args.source,log_dir, result_dir, config_dir))


    return args.source, args.result, args.concurrency, args.verbose


def log_init(log_dir):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=os.path.join(log_dir, "monitor.log"))

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(filename)s %(levelname)s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def datax_work(jobs, results):
    while True:
        try:
            datax_cfg, log_name, results_name = jobs.get()

            report("datax_cfg %s log_name %s results_name ..."%(datax_cfg, log_name, results_name))
            job_result = execute_job(datax_cfg, log_name, results_name)
            results.put((datax_cfg, job_result))
        except Exception as inst:
            report("Error ...", True)
            results.put((datax_cfg, 1))
        finally:
            jobs.task_done()


def create_processes(jobs, results, concurrency):
    for _ in range(concurrency):
        process = multiprocessing.Process(target=datax_work,
                                          args=(jobs, results))
        process.daemon = True
        process.start()

    process = multiprocessing.Process(target=report_results,
                                      args=("nouse", results))
    process.daemon = True
    process.start()


def datax_jobs(source, concurrency):
    jobs = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()
    create_processes(jobs, results, concurrency)

    wm = pyinotify.WatchManager()
    wm.add_watch(source, pyinotify.ALL_EVENTS, rec=True)
    eh = EventHandler(jobs)
    notifier = pyinotify.Notifier(wm, eh)

    try:
        notifier.loop()
        jobs.join()
    except KeyboardInterrupt:
        report("caceling...")
        notifier.stop()
    report("all jobs finished")


def main():
    source, result, concurrency, verbose = handle_commandline()
    if verbose:
        report("concurrency is %s"%(concurrency))
    report("starting...")
    datax_jobs(source, concurrency)


if __name__ == "__main__":
    main()
