import requests
import json
import re
import datetime
import query, utils
import logging
from bs4 import BeautifulSoup
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# use config_v3
# with open('config_v3.json', 'r', encoding='utf-8') as read_config:
#     data_config = json.load(read_config)


def detect_type_response(config):
    response = detect_type_crawl(config, config['url'])
    if config['type_response'] == 1:
        response = html.fromstring(response.text, 'lxml')
        list_data = parse_html(response, config)
    elif config['type_response'] == 2:
        list_data = parse_json(response.json(), config)
    return list_data



def parse_html(response, config):
    return crawl_table(response, config)


def parse_json(response, config):
    list_data = []
    for obj in response['data']['4265']['data']:
        data_sample = config['data_sample'].copy()
        new_obj = get_all_key_json(obj, {})
        for key, vals in config['data_sample'].items():
            if vals == "obj_json":
                obj_config = config['obj_json'][key]
                try:
                    data = new_obj[obj_config['key']]
                    data = detect_type_result(data, obj_config)
                except:
                    data = ""
            elif vals == "web":
                data = detect_type_result(response, config[key])
            else:
                data = detect_key(key, vals, data_sample, data_sample['keyword'])
                data_sample[key] = data
                continue

            if type(data) == list:
                    del data_sample[key]
                    for i in range(len(data)):
                        data_sample[key + "_" + str(i)] = data[i]
            else:
                data_sample[key] = data
        list_data.append(data_sample)
    return list_data


def get_all_key_json(obj, new_obj):
    for key, vals in obj.items():
        if isinstance(vals, str):
            if key in new_obj:
                temp = []
                if isinstance(new_obj[key], list):
                    temp.extend(new_obj[key])
                else:
                    temp.append(new_obj[key])
                temp.append(vals)
                new_obj[key] = temp
            else:
                new_obj[key] = vals
            
        elif isinstance(vals, int):
            if key in new_obj:
                temp = []
                if isinstance(new_obj[key], list):
                    temp.extend(new_obj[key])
                else:
                    temp.append(new_obj[key])
                temp.append(vals)
                new_obj[key] = temp
            else:
                new_obj[key] = vals
        elif isinstance(vals, dict):
            get_all_key_json(vals, new_obj)
        elif isinstance(vals, list):
            for k, v in vals.items():
                get_all_key_json(v, new_obj)
        else:
            new_obj[key] = ""
    return new_obj


# obj = {
# "away_team": {
# "logo": "https://is.vnecdn.net/objects/teams/9.png",
# "team_id": 9,
# "team_name": "Tây Ban Nha",
# "team_name_full": "Spain"
# },
# "elapsed": 120,
# "event_date": "2022-12-06T22:00:00+07:00",
# "event_timestamp": 1670338800,
# "first_half_start": 1670338800,
# "fixture_id": 977345,
# "home_team": {
# "logo": "https://is.vnecdn.net/objects/teams/31.png",
# "team_id": 31,
# "team_name": "Morocco",
# "team_name_full": "Morocco"
# },
# "league_id": 4265,
# "position": 60,
# "referee": "Fernando Rapallini, Argentina",
# "round": "Round of 16",
# "round_int": 16,
# "score": {
# "extratime": "0-0",
# "fulltime": "0-0",
# "halftime": "0-0",
# "penalty": "3-0"
# },
# "second_half_start": 1670342400,
# "status": "Match Finished",
# "status_short": "PEN",
# "venue": "Education City Stadium"
# }

# print(get_all_key_json(obj, {}))



def crawl_table(browser, config):
    lich_thi_dau = []
    url = config['url']
    list_table = detect_type_result(browser.xpath(config['table']['xpath']), config['table'])
    for table in list_table:
        list_row_table = detect_type_result(table.xpath(config['table']['row']['xpath']), config['table']['row'])
        for row in list_row_table:
            data_sample = config['data_sample'].copy()
            for key, val in config['data_sample'].items():
                if val == "web":
                    data = detect_type_result(html_find_xpath(browser, config[key]), config[key])
                elif val == "table":
                    data = detect_type_result(html_find_xpath(table, config['table'][key]), config['table'][key])
                elif val == "row":
                    data = detect_type_result(html_find_xpath(row, config['table']['column'][key]), config['table']['column'][key])
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


def html_find_xpath(browser, config):
    try:
        return browser.xpath(config['xpath'])
    except:
        return ""


def detect_type_crawl(data_config, url):
    if data_config['type_crawl'] == 1:
        response = requests.get(url)
        return response
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
            try:
                list_key_unsign.append(utils.unicode_to_kodauvagach(key))
            except:
                logging.warning(data_sample)
        return list_key_unsign
    else:
        return None


def detect_type_result(result, config):
    try:
        type_result = config['type_result'] 
        if type_result == 1:
            return elements_to_output(result, config)
        elif type_result == 2:
            return list_string_to_output(result, config)
        elif type_result == 3:
            return list_int_to_output(result, config)
        elif type_result == 4:
            return string_to_output(result, config)
        elif type_result == 5:
            return int_to_output(result, config)
        elif type_result == 6:
            return datetime_to_output(result, config)
        elif type_result == 7:
            return timestamp_to_output(result, config)
    except:
        return config


def elements_to_output(obj, config):
    if config['type_output'] == 1: 
        return obj
    elif config['type_output'] == 2:
        pass
    elif config['type_output'] == 3:
        pass
    elif config['type_output'] == 4:
        pass
    elif config['type_output'] == 5:
        pass
    elif config['type_output'] == 6:
        pass
    pass


def list_string_to_output(obj, config):
    if config['type_output'] == 2:
        result = detect_type_find(obj, config)
        return [i.strip() for i in result if len(i.strip()) > 0]
    elif config['type_output'] == 3:
        list_numb = detect_type_find(remove_space("".join(obj)).strip(), config)
        return [int(i) for i in list_numb]
    elif config['type_output'] == 4:
        string = detect_type_find(remove_space("".join(obj)).strip(), config)
        return string
    elif config['type_output'] == 5:
        numb = detect_type_find(remove_space("".join(obj)).strip(), config)
        return int(numb)
    elif config['type_output'] == 6:
        time = detect_type_find(remove_space("".join(obj)).strip(), config)
        return detect_time_format(time, config)


def list_int_to_output(obj, config):
    pass


def string_to_output(obj, config):
    if config['type_output'] == 3:
        obj = detect_type_find(remove_space("".join(obj).strip()), config)
        return [int(i) for i in obj]
    elif config['type_output'] == 4:
        obj = detect_type_find(remove_space("".join(obj)).strip(), config)
        return obj
    elif config['type_output'] == 5:
        obj = detect_type_find(remove_space("".join(obj)).strip(), config)
        return int(obj)
    elif config['type_output'] == 2:
        pass
    elif config['type_output'] == 6:
        pass


def int_to_output(obj, config):
    if config['type_output'] == 5:
        return obj
    elif config['type_output'] == 2:
        pass
    elif config['type_output'] == 3:
        pass
    elif config['type_output'] == 4:
        pass
    elif config['type_output'] == 6:
        pass


def datetime_to_output(obj, config):
    pass


def timestamp_to_output(obj, config):
    if config['type_output'] == 6:
        obj = detect_type_find(obj, config)
        if isinstance(obj, dict):
            obj = int(obj[0])
        elif isinstance(obj, str):
            obj = int(obj)
        obj = datetime.datetime.fromtimestamp(obj)
        return obj
    elif config['type_output'] == 3:
        pass
    elif config['type_output'] == 4:
        pass
    elif config['type_output'] == 5:
        pass
    elif config['type_output'] == 2:
        pass


def detect_type_find(obj, config):
    if config['type_find'] == 1: # giữ nguyên obj
        return obj
    elif config['type_find'] == 2: # tìm theo regex
        return regex_extract(obj, config)


def regex_extract(obj, config):
    regex = config['re']
    result = re.findall(regex, obj)
    return result


def detect_time_format(time, config):
    if type(time) == list:
        time = time[0]
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
    return time


def remove_space(string):
    return re.sub('\s+', ' ', string)


def main(type, es, index_es, config):
    list_data = detect_type_response(config)
    if type == 2:
        check_update = query.update_lich_ES(es, index_es, list_data)
    elif type == 5:
        check_update = query.update_bxh_ES(es, index_es, list_data)
    return check_update

# def main(config):
#     list_data = detect_type_response(config)
#     print(list_data)

# with open('config_test.json', 'r', encoding='utf-8') as read_config:
#     data_config = json.load(read_config)

# main(data_config[0]['bang_xep_hang'][0])

# wc, col_config = query.connect_DB_aHuy()
# list_config = col_config.find({})
# config = list_config[0]['lich_thi_dau'][0]
# es = query.connect_ES()
# index_es = "worldcup"
# list_data = crawl_table(config)
# check_update = query.update_lich_ES(es, index_es, list_data)

# wc, col_config = query.connect_DB_aHuy()
# list_config = col_config.find({})
# config = list_config[0]['bang_xep_hang'][0]
# es = query.connect_ES()
# index_es = "worldcup"
# list_data = crawl_table(config)
# check_update = query.update_bxh_ES(es, index_es, list_data)
# print(check_update)
