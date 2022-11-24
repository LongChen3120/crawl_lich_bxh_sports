import crawl_lich_bxh, query
import json
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
    logging.warning(f"thread {threading.Thread.name} got config type:{config['type']}")
    es = query.connect_ES()
    time_check_update = 121
    count = 0
    while time_check_update > 0:
        count += 1
        logging.warning(f"_{count}_, config type:{config['type']}")
        check_update = crawl_lich_bxh.main(config['type'], es, index_es, config)
        if check_update == True:
            break
        else:
            logging.warning(f"Not found update !")
            if time_check_update > 120:
                time.sleep(3 * 60)
                time_check_update -= 3
            else:
                time.sleep(30 * 60)
                time_check_update -= 30


def detect_time_run(index_es):
    es = query.connect_ES()
    time_now = datetime.datetime.now()
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
                                "gte": time_now
                            }
                        }
                    }
                ]
            }
        }
    }
    result = es.search(index=index_es, body=query_search)
    time = datetime.datetime.strptime(result['hits']['hits'][0]['_source']['time'], "%Y-%m-%dT%H:%M:%S")
    time_run = time + datetime.timedelta(minutes=120)
    logging.warning(f"next run at:{time_run}")
    return time_run


def crawl_handler(index_es):
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
    
    time_next_run = detect_time_run(index_es)
    scheduler_run(time_next_run, index_es)


def read_config():
    queue_config = queue.Queue()
    World_cup_2022, config = query.connect_DB_local()
    list_config = config.find({})
    for league in list_config:
        for config_lich in league['lich_thi_dau']:
            queue_config.put(config_lich)
        for config_bxh in league['bang_xep_hang']:
            queue_config.put(config_bxh)
    return queue_config


def scheduler_run(time_run, index_es):
    scheduler = BackgroundScheduler()
    scheduler.start()

    trigger = CronTrigger(
        year="*", month="*", day="*", hour=time_run.hour, minute=time_run.minute, second=time_run.second
    )
    scheduler.add_job(
        crawl_handler,
        trigger=trigger,
        args=[index_es]
    )
    while True:
        time.sleep(1)


if __name__ == '__main__':
    start_time = time.time()

    logging.basicConfig(filename='.\log\log_main.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.WARNING)
    logging.warning("\n\n_____________________________new_log_____________________________")

    index_es = "worldcup"
    next_start = detect_time_run(index_es)
    scheduler_run(next_start, index_es)
    
    print("done ! \ntime: ",(time.time() - start_time)) 