import crawl_lich_bxh, query
import json
import setting
import logging
import datetime, time
import queue
import threading
import requests
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
        # check_update = False
        if check_update == True:
            logging.warning(f"update ok__ config type: {config['type']}, domain: {config['domain']}")
            break
        else:
            logging.warning(f"Not found update !__ config type: {config['type']}, domain: {config['domain']}")
            if time_check_update > 360:
                time.sleep(3 * 60)
                time_check_update -= 3
            elif time_check_update > 1:
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
    time_start = datetime.datetime.strptime(result['hits']['hits'][0]['_source']['time'], "%Y-%m-%dT%H:%M:%S")
    time_run = get_time_run(time_start, 120)
    call_api_set_lich_tuong_thuat(time_start, result['hits']['hits'][0]['_source'])
    logging.warning(f"next run at:{time_run}, match id: {result['hits']['hits'][0]['_id']}")
    return result['hits']['hits'][0]['_id'], time_start, time_run


def call_api_set_lich_tuong_thuat(time_start, doc):
    logging.info(doc)
    time_start_crawl = get_time_run(time_start, -30)
    time_start_crawl = datetime.datetime.strftime(time_start_crawl, "%Y-%m-%dT%H:%M:%S.%f")
    payload = setting.PAYLOAD
    payload['start_time'] = str(time_start_crawl)
    payload['site'] = doc['domain']
    payload['kwargs'] = {
        "round": doc['round'],
        "team_1": doc['team_0'],
        "team_2": doc['team_1']
    }
    try:
        res = requests.post(setting.API, headers=setting.HEADER, json=payload)
        logging.warning(f"__response: {res.text}\nstart run crawl thuong thuat luc:{time_start_crawl},")
    except:
        logging.warning("exception: ", exc_info=True)


def crawl_handler(time_start, index_es):
    queue_config = read_config()
    list_thread = []
    # while queue_config.empty() == False:
    #     config = queue_config.get()
    #     scheduler_update(config)
    while queue_config.empty() == False:
        config = queue_config.get()
        thread = My_thread(config, index_es)
        thread.daemon
        thread.start()
        list_thread.append(thread)

    id, time_start, time_run = detect_time_run(index_es, time_start)
    add_job(id, time_start, time_run, index_es)
    for thread in list_thread:
        thread.join()


def get_time_now():
    return datetime.datetime.now()


def get_time_run(time_start, time_match):
    return (time_start + datetime.timedelta(minutes=time_match))


def read_config():
    queue_config = queue.Queue()
    # with open('config_v3.json', 'r', encoding='utf-8') as rf:
    #     list_config = json.load(rf)
    World_cup_2022, config = query.connect_DB_aHuy()
    list_config = config.find({})
    for league in list_config:
        for config_lich in league['lich_thi_dau']:
            queue_config.put(config_lich)
        # for config_bxh in league['bang_xep_hang']:
        #     queue_config.put(config_bxh)
    return queue_config


def add_job(id, time_start, time_run, index_es):
    trigger = CronTrigger(year="*", month="*", day="*", hour=time_run.hour, minute=time_run.minute, second=time_run.second)
    scheduler.add_job(crawl_handler, trigger=trigger, args=[time_start, index_es], max_instances=10, name=f"job: {id}")


def scheduler_run(id, time_start, time_run, index_es):
    global scheduler
    scheduler = BackgroundScheduler()
    scheduler.start()
    
    # crawl_handler(time_start, index_es)
    add_job(id, time_start, time_run, index_es)
    while True:
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    start_time = time.time()

    logging.basicConfig(filename='.\log\log_main.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.WARNING)
    logging.warning("\n\n_____________________________new_log_____________________________")

    index_es = "worldcup"
    list_time_run = []

    # first run: update and insert.
    queue_config = read_config()
    es = query.connect_ES()
    list_thread = []
    while queue_config.empty() == False:
        config = queue_config.get()
        # crawl_lich_bxh.main(config['type'], es, index_es, config)
        logging.warning(f"thread {threading.Thread.name} got config type:{config['type']}, domain: {config['domain']}")
        t = threading.Thread(target=crawl_lich_bxh.main, args=(config['type'], es, index_es, config, ))
        t.daemon
        t.start()
        list_thread.append(t)
    for thread in list_thread:
        thread.join()

    # run scheduler
    # time_now = get_time_now()
    # id, time_start, time_run = detect_time_run(index_es, time_now)
    # scheduler_run(id, time_start, time_run, index_es)
    
    print("done ! \ntime: ",(time.time() - start_time)) 