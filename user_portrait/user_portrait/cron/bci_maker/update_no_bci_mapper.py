# -*-coding:utf-8-*-
import time
import sys
import json
import numpy as np
from myconfig import * 
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
reload(sys)
sys.path.append('../../../')
from global_utils import es_user_portrait as es_9200
from global_utils import es_user_profile
from global_utils import flow_text_index_name_pre, flow_text_index_type
from global_utils import redis_flow_text_mid as r_flow
from parameter import DAY, RUN_TYPE
from time_utils import ts2datetime, datetime2ts, ts2date


def mapper_bci_history(todaydate=None):
    if todaydate:
        TODAY_TIME = todaydate
    es_query = {"query":{"bool":{"must_not":[{"term":{"bci.update_time":TODAY_TIME}}]}},"fields":["user_fansnum"], "size":1000}

    s_re = scan(es_user_profile, query=es_query, index=BCIHIS_INDEX_NAME, doc_type=BCIHIS_INDEX_TYPE)
    count = 0
    array = []
    while 1:
        try:
            temp = s_re.next()
            one_item = {}
            one_item['id'] = temp['_id'].encode("utf-8")
            one_item['total_num'] = 0
            one_item['today_bci'] = 0
            one_item['update_time'] = TODAY_TIME
            tmp = temp.get('fields', {})
            if tmp:
                one_item['user_fansnum'] = temp['fields']["user_fansnum"][0]
            else:
                one_item['user_fansnum'] = 0
            array.append(one_item)
            count += 1
            if count % 1000 == 0:
                r_flow.lpush('update_bci_list', json.dumps(array))
                array = []
                if count % 100000 == 0:
                    print count
        except StopIteration: 
                print "all done" 
                if array:
                    r_flow.lpush('update_bci_list', json.dumps(array))
                break 

    print count


if __name__ == '__main__':
    todaydate = ts2datetime(time.time())
    todaydate = "2016-04-18"
    print todaydate
    lenth = r_flow.llen('update_bci_list')
    if not lenth:
        print "cron/scan/history_bci&start&%s" %ts2date(time.time())
        mapper_bci_history(todaydate)
        print "cron/scan/history_bci&end&%s" %ts2date(time.time())
    else:
        time.sleep(60)
