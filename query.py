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
    db_wc = client["World_Cup_2022"]
    lich_thi_dau = db_wc["lich_thi_dau"]
    bang_xep_hang = db_wc["bang_xep_hang"]
    return lich_thi_dau, bang_xep_hang


def connect_DB_aHuy():
    client = pymongo.MongoClient("mongodb://192.168.19.168:27017")
    db_paper = client['PaPer']
    World_cup_2022 = db_paper['World_cup_2022']
    return World_cup_2022


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


def update_lich_ES(es, es_index, list_data):    
    check_update = False
    for match in list_data:
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
        if result['hits']['total']['value'] == 1:
            del match['create_date']
            id_match = result['hits']['hits'][0]['_id']
            query_update = {
                "doc":match
            }
            response = es.update(index=es_index, id=id_match, body=query_update)
            if response['result'] == "updated":
                logging.info(f"update lich ok, search after update:{match}")
                check_update = True
        else:
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
            id_match = result['hits']['hits'][0]['_id']
            query_update = {
                "doc":team
            }
            response = es.update(index=es_index, id=id_match, body=query_update)
            if response['result'] == "updated":
                logging.info(f"update bxh ok, search after update:{team}")
                check_update = True
        else:
            logging.info(f"insert match:{team}")
            es.index(index=es_index, body=team)
    return check_update

# try:
#     es = connect_ES()
#     es.indices.create(index="test_insert_es")
# except:
#     pass


# es = connect_ES()
# match = {
#     "topic" : "World cup 2022",
#     "type" : 5,
#     "group" : "Bảng A",
#     "team" : "Hà Lan",
#     "rank" : 1,
#     "matches_played" : 1,
#     "wins" : 1,
#     "draws" : 0,
#     "losses" : 0,
#     "goals" : 2,
#     "goals_conceded" : 0,
#     "win_lose_difference" : 2,
#     "points" : 3,
#     "url" : "https://www.24h.com.vn/bong-da/bang-xep-hang-bong-da-world-cup-2022-c48a1369009.html",
#     "keyword" : [
#     "World cup 2022",
#     "Bảng xếp hạng world cup 2022",
#     "Bảng A",
#     "Hà Lan"
#     ],
#     "keyword_unsignn" : [
#     "world-cup-20222",
#     "bang-xep-hang-world-cup-2022",
#     "bang-a",
#     "ha-lan"
#     ],
#     "create_date" : "2022-11-22T11:18:55.570000",
#     "publish_date" : "2022-11-22T05:21:00",
#     "domain" : "https://www.24h.com.vn/",
#     "thread" : ""
# }

# result = update_bxh_ES(es, "test_update_bxh", [match])
# print(result)