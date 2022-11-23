import pymongo
from elasticsearch import Elasticsearch
import json
import datetime
import logging

# _________________ MongoDB _________________
# local mongodb://localhost:27017
# a huy mongodb://192.168.19.168:27017
def connect_DB_local():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db_wc = client["PaPer"]
    World_cup_2022 = db_wc["World_cup_2022"]
    config = db_wc["config_crawl_sport"]
    return World_cup_2022, config


def connect_DB_aHuy():
    client = pymongo.MongoClient("mongodb://192.168.19.168:27017")
    db_paper = client['PaPer']
    World_cup_2022 = db_paper['World_cup_2022']
    config = db_paper["config_crawl_sport"]
    return World_cup_2022, config


def insert_DB(col, list_data):
    col.insert_many(list_data)


def update_lich_DB(col, list_data):
    for match in list_data:
        filter = {"team_0":match['team_0'], "team_1":match['team_1'],"time":match['time'], "domain":match['domain']}
        if col.find_one(filter):
            vals = {"$set":match}
            col.update_one(filter, vals)
        else:
            col.insert_one(match)


def update_bxh_DB(col, list_data):
    for team in list_data:
        filter = {"group":team['group'], "team":team['team'], "domain":team['domain']}
        if col.find_one(filter):
            vals = {"$set":team}
            col.update_one(filter, vals)
        else:
            col.insert_one(team)


# def update_config():
#     wc22, col_config =connect_DB_aHuy()
#     with open('config_v3.json', 'r', encoding='utf-8') as read_config:
#         data_config = json.load(read_config)
#     for config in data_config:
#         try:
#             if col_config.find_one({"league":config['league']}):
#                 mapping_site = {"league":{"$regex":f"{config['league']}"}}
#                 update_vals = {"$set":config}
#                 col_config.update_one(mapping_site, update_vals)
#             else:
#                 col_config.insert_one(config)
#         except:
#             print(config)
# update_config()

# _________________ ES _________________
# a huy : 192.168.19.168:49154
# local : http://127.0.0.1:9200/
# a hoat: http://192.168.19.77:9200/
# server: http://10.3.11.253:3008
def connect_ES():
    es = Elasticsearch(hosts="http://127.0.0.1:9200/")
    return es


def insert_ES(es, es_index, list_data):
    for match in list_data:
        es.index(index=es_index, body=match)


def update_lich_ES(es, es_index, list_data, id_match_need_update):    
    check_update = False
    match_need_update = es.get(index=es_index, id=id_match_need_update)['_source']
    for match in list_data:
        if match['team_0'] == match_need_update['team_0'] and match['team_1'] == match_need_update['team_1']:
            del match['create_date']
            match['last_update'] = datetime.datetime.now()
            query_update = {
                "doc":match
            }
            response = es.update(index=es_index, id=id_match_need_update, body=query_update)
            if response['result'] == "updated":
                logging.info(f"update lich ok, match_new:{match}")
                check_update = True
                break
        else:
            query = {
                "query": {
                    "bool": {
                        "must":[
                            {
                                "match":{
                                    "type":match['type']
                                }
                            },
                            {
                                "match":{
                                    "team_0":{
                                        "query":match['team_0'],
                                        "operator" : "AND"
                                    }
                                }
                            },
                            {
                                "match":{
                                    "team_1":{
                                        "query":match['team_1'],
                                        "operator" : "AND"
                                    }
                                }
                            },
                            {
                                "match":{
                                    "time":match['time']
                                }
                            },
                            {
                                "match":{
                                    "domain":match['domain']
                                }
                            }
                        ]
                    }
                }            
            }
            result =  es.search(index=es_index, body=query)
            if result['hits']['total']['value'] == 0:
                logging.info(f"insert match:{match}")
                es.index(index=es_index, body=match)
    return check_update


def update_bxh_ES(es, es_index, list_data):
    check_update = False
    for team in list_data:
        query_find = {
            "query": {
                "bool": {
                    "must":[
                        {
                            "match":{
                                "group":{
                                    "query":team['group'],
                                    "operator" : "AND"
                                }
                            }
                        },
                        {
                            "match":{
                                "team":{
                                    "query":team['team'],
                                    "operator" : "AND"
                                }
                            }
                        },
                        {
                            "match":{
                                "domain":{
                                    "query":team['domain'],
                                    "operator" : "AND"
                                }
                            }
                        }
                    ]
                }
            }            
        }
        result =  es.search(index=es_index, body=query_find)
        if result['hits']['total']['value'] == 1:
            del team['create_date']
            team['last_update'] = datetime.datetime.now()
            id_match = result['hits']['hits'][0]['_id']
            query_update = {
                "doc":team
            }
            response = es.update(index=es_index, id=id_match, body=query_update)
            if response['result'] == "updated":
                logging.info(f"update bxh ok, search after update:{team}")
                check_update = True
                break
        else:
            logging.info(f"insert match:{team}")
            es.index(index=es_index, body=team)
    return check_update




# es = connect_ES()
# query = {
#     "size": 65, 
#     "sort": [
#         {
#             "create_date": {
#                 "order": "desc"
#             }
#         }
#     ],
#     "query": {
#         "bool": {
#           "must": [
#             {
#               "match": {
#                 "type": 2
#               }
#             },
#             {
#               "range": {
#                   "time":{
#                       "gte": "2022-11-20T13:07:00"
#                   }
#               }
#             }
#           ]
#         }
#     }
# }
# result = es.search(index="worldcup", body=query)['hits']['hits']

# local_es = Elasticsearch(hosts="http://127.0.0.1:9200/")
# # try:
# #     local_es.indices.create(index="worldcup")
# # except:
# #     pass
# for match in result:
#     local_es.index(index="worldcup", body=match['_source'])
# # print(len(result))