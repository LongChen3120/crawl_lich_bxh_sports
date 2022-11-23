import crawl_lich_bxh
import json


with open('config_v1.json', 'r', encoding='utf-8') as read_config:
    data_config = json.load(read_config)

def update_lich_thi_dau():
    list_lich_thi_dau = crawl_lich_bxh.crawl_lich_thi_dau(data_config[0])
    for match in list_lich_thi_dau:
        print(match)