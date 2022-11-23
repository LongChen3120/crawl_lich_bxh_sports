import requests
import json
import re
import datetime
import query, utils
from bs4 import BeautifulSoup
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# use config_v3
with open('config_v3.json', 'r', encoding='utf-8') as read_config:
    data_config = json.load(read_config)

def crawl_table(config):
    lich_thi_dau = []
    url = config['url']
    browser = detect_type_crawl(config, url)
    # with open('D:\congTacVienVCC\crawl_sports\lich.html', 'r', encoding='utf-8') as read_html:
    #     browser = read_html.read()
    #     browser = html.fromstring(browser, 'lxml')
    list_table = detect_type_result(browser, config['table'])
    for table in list_table:
        list_row_table = detect_type_result(table, config['table']['row'])
        for row in list_row_table:
            data_sample = config['data_sample'].copy()
            for key, val in config['data_sample'].items():
                if val == "lich_thi_dau" or val == "bang_xep_hang":
                    data = detect_type_result(browser, config[key])
                elif val == "table":
                    data = detect_type_result(table, config['table'][key])
                elif val == "column":
                    data = detect_type_result(row, config['table']['column'][key])
                else:
                    data = detect_key(key, val, data_sample, data_sample['keyword'])
                    data_sample[key] = data
                    continue
                
                if type(data) == list:
                    del data_sample[key]
                    for i in range(len(data)):
                        data_sample[key + "_" + str(i)] = data[i]
                else:
                    data_sample[key] = data
            lich_thi_dau.append(data_sample)
    return lich_thi_dau


def detect_type_crawl(data_config, url):
    if data_config['type_crawl'] == 1:
        response = requests.get(url)
        browser = html.fromstring(response.text, 'lxml')
        return browser
    elif data_config['type_crawl'] == 2:
        options = Options()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.headless = False
        browser = webdriver.Chrome(executable_path='./chrome_driver/chromedriver.exe', options=options)
        browser.implicitly_wait(10)
        browser.get(url)
        return browser


def detect_key(key, vals, data_sample, list_key):
    if key == "create_date":
        return datetime.datetime.now()
    elif key == "keyword":
        list_keyword = []
        for val in vals:
            try:
                list_keyword.append(data_sample[val])
            except:
                list_keyword.append(val)
        return list_keyword
    elif key == "keyword_unsign":
        list_key_unsign = []
        for key in list_key:
            list_key_unsign.append(utils.unicode_to_kodauvagach(key))
        return list_key_unsign
    else:
        return vals

def detect_level(field):
    if field['level'] == 1:
        pass
    elif field['level'] == 2:
        pass
    elif field['level'] == 3:
        pass

def detect_type_result(browser, config):
    try:
        if config['type_result'] == 0:
            list_box_match = browser.xpath(config['xpath'])
            return list_box_match
        elif config['type_result'] == 1:
            round_name = browser.xpath(config['xpath'])[0]
            round_name = round_name.text_content().strip()
            return re.sub('\s+', ' ', round_name)
        elif config['type_result'] == 2:
            round_name = browser.xpath(config['xpath'])
            return round_name
        elif config['type_result'] == 3:
            round_name = browser.xpath(config['xpath'])[0]
            round_name = round_name.text_content().strip()
            return int(re.sub('\s+', ' ', round_name))
        elif config['type_result'] == 4:
            round_name = browser.xpath(config['xpath'])
            try:
                list_result = [int(i.strip()) for i in round_name]
            except:
                list_result = [i.strip() for i in round_name]
            return list_result
        elif config['type_result'] == 5:
            round_name = browser.xpath(config['xpath'])[0]
            round_name = round_name.text_content().strip()
            time = detect_time_format(re.sub('\s+', ' ', round_name), config)
            return time
    except: 
        return config

def detect_time_format(time, config):
    try:
        try:
            time = re.findall(config['re'], time)[0]
        except:
            pass
        time_format = config['time_format'].replace("days","%d").replace("months","%m").replace("years","%Y").replace("hours","%H").replace("minutes","%M").replace("seconds","%S")
        time = datetime.datetime.strptime(time, time_format)
        try:
            params = config['replace'].split('=')
            if params[0] == "years":
                time = time.replace(year=int(params[1]))
            elif params[0] == "months":
                time = time.replace(month=int(params[1]))
            elif params[0] == "days":
                time = time.replace(day=int(params[1]))
            elif params[0] == "hours":
                time = time.replace(hour=int(params[1]))
        except:
            pass
    except:
        pass
    return time


def main(type, es, index_es, config, id_match_need_update):
    list_data = crawl_table(config)
    if type == 2:
        check_update = query.update_lich_ES(es, index_es, list_data, id_match_need_update)
    elif type == 5:
        check_update = query.update_bxh_ES(es, index_es, list_data)
    return check_update


# wc, col_config = query.connect_DB_local()
# list_config = col_config.find({})
# config = list_config[0]['lich_thi_dau'][0]
# es = query.connect_ES()
# index_es = "test_insert_es"
# list_data = crawl_table(config)
# id_match_need_update = "abc"
# check_update = query.update_lich_ES(es, index_es, list_data, id_match_need_update)
