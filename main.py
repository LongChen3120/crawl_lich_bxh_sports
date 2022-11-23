import crawl_lich_bxh, query
import json
import logging
import datetime, time
import queue
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

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
            "range": {
                "time":{
                    "gte": time_now
                }
            }
        }
    }
    result = es.search(index=index_es, body=query_search)
    logging.info(f"search before update:{result}")
    time = datetime.datetime.strptime(result['hits']['hits'][0]['_source']['time'], "%Y-%m-%dT%H:%M:%S")
    time_run = time + datetime.timedelta(minutes=1)
    logging.info(f"next run at:{time_run}")
    return time_run

def crawl_handler(index_es):
    count_run = 0
    while True:
        count_run += 1
        logging.info(f"run:{count_run}")
        check_update_lich, check_update_bxh = crawl_lich_bxh.main(index_es)
        if check_update_lich == True and check_update_bxh == True:
            break
        time.sleep(180)
    time_run = detect_time_run(index_es)
    print("call back main")
    main(time_run, index_es)
    


def main(time_run, index_es):
    # time_next = detect_time_run(index_es)
    # crawl_lich_bxh.main(index_es)

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
        time.sleep(5)


if __name__ == '__main__':

    # time_now = datetime.datetime.now()
    # print(time_now)
    # time_run = time_now + datetime.timedelta(minutes=90)

    # print(time_run)
    start_time = time.time()

    logging.basicConfig(filename='D:\congTacVienVCC\crawl_sports\log\log_main.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("\n\n_____________________________new_log_____________________________")

    with open("D:\congTacVienVCC\crawl_sports\config.json", 'r', encoding='utf-8') as read_config:
        data_config = json.load(read_config)

    index_es = "test_insert_es"
    next_start = detect_time_run(index_es)
    main(next_start, index_es)
    
    print("done ! \ntime: ",(time.time() - start_time)) 