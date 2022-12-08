import crawl_lich_bxh, query
import json
import os
import logging
import datetime, time
import queue
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


class My_thread(threading.Thread):
    def __init__(self, config, index_es):
        threading.Thread.__init__(self)
        self.config = config
        self.index_es = index_es

    def run(self):
        scheduler_update(self.config, self.index_es)


def scheduler_update(config, index_es):

    es = query.connect_ES()
    time_check_update = 480
    count = 0
    while time_check_update > 0:
        count += 1
        logging.warning(f"_count_{count}: thread {threading.Thread.name} got config type:{config['type']}, domain: {config['domain']}")
        check_update = crawl_lich_bxh.main(config['type'], es, index_es, config)
        if check_update == True:
            logging.warning(f"update ok__ config type: {config['type']}, domain: {config['domain']}")
            break
        else:
            logging.warning(f"Not found update !__ config type: {config['type']}, domain: {config['domain']}")
            # break
            if time_check_update > 120:
                time.sleep(3 * 60)
                time_check_update -= 3
            else:
                time.sleep(30 * 60)
                time_check_update -= 30


def detect_time_run(index_es, time_now):
    es = query.connect_ES()
    query_search = {
        "size": 1, 
        "sort": [
            {
                "time": {
                    "order": "asc"
                }
            }
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "type": 2
                        }
                    },
                    {
                        "range": {
                            "time":{
                                "gt": time_now
                            }
                        }
                    }
                ]
            }
        }
    }
    result = es.search(index=index_es, body=query_search)
    time_match = datetime.datetime.strptime(result['hits']['hits'][0]['_source']['time'], "%Y-%m-%dT%H:%M:%S")
    time_run = time_match + datetime.timedelta(minutes=120)
    logging.warning(f"next run at:{time_run}, update match id: {result['hits']['hits'][0]['_id']}")
    return time_run, time_match


def crawl_handler(time_match, index_es):
    queue_config = read_config()
    list_thread = []
    # while queue_config.empty() == False:
    #     config = queue_config.get()
    #     scheduler_update(config, index_es)
    while queue_config.empty() == False:
        config = queue_config.get()
        thread = My_thread(config, index_es)
        thread.daemon
        thread.start()
        list_thread.append(thread)

    for thread in list_thread:
        thread.join()
    
    time_run, time_match = detect_time_run(index_es, time_match)
    scheduler_run(time_run, time_match, index_es)


def read_config():
    queue_config = queue.Queue()
    World_cup_2022, config = query.connect_DB_aHuy()
    list_config = config.find({})
    for league in list_config:
        for config_lich in league['lich_thi_dau']:
            queue_config.put(config_lich)
        for config_bxh in league['bang_xep_hang']:
            queue_config.put(config_bxh)
    return queue_config


def scheduler_run(time_run, time_match, index_es):
    scheduler = BackgroundScheduler()
    scheduler.start()
    # crawl_handler(time_match, index_es)
    trigger = CronTrigger(
        year="*", month="*", day="*", hour=time_run.hour, minute=time_run.minute, second=time_run.second
    )
    scheduler.add_job(
        crawl_handler,
        trigger=trigger,
        args=[time_match, index_es],
        max_instances=10
    )
    while True:
        time.sleep(1)


if __name__ == '__main__':
    start_time = time.time()
    with open('.\log\pid.txt', 'w', encoding='utf-8') as wf:
        wf.write(str(os.getpid()) + "\n")

    logging.basicConfig(filename='.\log\log_main.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.WARNING)
    logging.warning("\n\n_____________________________new_log_____________________________")

    index_es = "worldcup"
    time_now = datetime.datetime.now()
    time_run, time_match = detect_time_run(index_es, time_now)
    scheduler_run(time_run, time_match, index_es)
    
    print("done ! \ntime: ",(time.time() - start_time)) 