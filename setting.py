API = "http://192.168.19.168:5005/schedule_crawl"
HEADER = {"Content-Type": "application/json"}
PAYLOAD = {
    "key": "worldcup_live",
    "type": 1,
    "start_time": "2022-12-08T16:36:08.187808",
    "interval": 30,
    "auto_controll_interval": "False",
    "site": "https://vtc.vn/",
    "topic": "World Cup Live",
    "kwargs": {"test": "huy"},
    "loops": 6
}