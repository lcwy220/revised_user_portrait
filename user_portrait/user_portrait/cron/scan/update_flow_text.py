# -*-coding:utf-8-*-

import time
import sys
import json
import redis
from elasticsearch import Elasticsearch
reload(sys)
sys.path.append('../../')
from global_utils import R_CLUSTER_FLOW1 as r
from global_utils import es_flow_text as es_text
from global_utils import redis_flow_text_mid as r_flow
from global_utils import flow_text_index_name_pre, flow_text_index_type
from parameter import DAY, RUN_TYPE
from time_utils import ts2datetime, datetime2ts

def main():
    if RUN_TYPE:
        ts = time.time() - DAY
        date = ts2datetime(ts)
    else:
        date = '2013-09-05'
    index_name = flow_text_index_name_pre+date

    print index_name
    tb = time.time()
    count = 0
    while 1:
        user_set = r_flow.rpop('update_mid_list')
        if user_set:
            bulk_action = json.loads(user_set)
            es_text.bulk(bulk_action, index=index_name, doc_type=flow_text_index_type, timeout=600)
            count += 1000
            ts = time.time()
            if count % 1000000:
                print "%s : %s" %(count, ts - tb)
                tb = ts
        else:
            #break
            time.sleep(100)
            #print "waiting"

if __name__ == "__main__":
    time.sleep(60)
    main()


