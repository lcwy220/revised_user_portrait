# -*-coding:utf-8

import sys
import time
import redis
import json

reload(sys)
sys.path.append('./../../')
from global_utils import  R_CLUSTER_FLOW1 as r
from time_utils import ts2datetime, datetime2ts, ts2date
from parameter import TIME_INTERVAL

def get_queue_index(timestamp):
    time_struc = time.gmtime(float(timestamp))
    hour = time_struc.tm_hour
    minute = time_struc.tm_min
    index = hour*4+math.ceil(minute/15.0) #every 15 minutes
    return int(index)

if __name__ == "__main__":
    now_ts = time.time()
    date_ts = datetime2ts(ts2datetime(now_ts))
    if now_ts - TIME_INTERVAL < date_ts:
        sys.exit(0)

    tmp_date = ts2date(now_ts)
    print "cron_influence_start_" + tmp_date
    index = get_queue_index(now_ts) #当前时间戳所对应的时间区间
    influence_ts = "influence_timestamp_" + str(index)
    scan_cursor = 0
    count = 0
    while 1:
        re_scan = r.hscan(influence_ts, scan_cursor, count=1000)
        scan_cursor = re_scan[0]
        detail = re_scan[1]
        if len(detail):
            for k,v in detail.iteritems():
                r.zadd(influence_ts, v, k)
                count += 1
        if int(scan_cursor) == 0:
            break

    tmp_date = ts2date(time.time())
    print count
    print "cron_influence_end_" + tmp_date


