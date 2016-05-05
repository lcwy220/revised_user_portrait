# -*- coding: utf-8 -*-

import os
from flask import Flask

REDIS_CLUSTER_HOST_FLOW1 = '10.128.55.67'
#REDIS_CLUSTER_HOST_FLOW1_LIST = ["219.224.134.211", "219.224.134.212", "219.224.134.213"]
REDIS_CLUSTER_PORT_FLOW1 = '6379'
#REDIS_CLUSTER_PORT_FLOW1_LIST = ["6379", "6380"]
REDIS_CLUSTER_HOST_FLOW2 = '10.128.55.68'
REDIS_CLUSTER_PORT_FLOW2 = '6379'
REDIS_CLUSTER_HOST_FLOW3 = '10.128.55.71'
REDIS_CLUSTER_PORT_FLOW3 = '6379'
REDIS_HOST = '10.128.55.68'
REDIS_PORT = '6379'
#uname to uid 
UNAME2UID_HOST = '10.128.55.72'
UNAME2UID_PORT = '6379'
# uname2uid in redis: {'weibo_user': {uname:uid, ...}}
UNAME2UID_HASH = 'weibo_user'
REDIS_TEXT_MID_HOST = '10.128.55.67'
REDIS_TEXT_MID_PORT = "6379"

#flow3:retweet/be_retweet redis
RETWEET_REDIS_HOST = '10.128.55.69'
RETWEET_REDIS_PORT = '6379'
#flow3:comment/be_comment redis
COMMENT_REDIS_HOST = '10.128.55.70'
COMMENT_REDIS_PORT = '6379'


#USER_ES_HOST = '219.224.135.97'
ES_CLUSTER_HOST_FLOW1 = ["10.128.55.69", "10.128.55.70", "10.128.55.71"]
ES_COPY_USER_PORTAIT_HOST = ["10.128.55.69", "10.128.55.70", "10.128.55.71"]
ZMQ_VENT_PORT_FLOW1 = '6387'
ZMQ_CTRL_VENT_PORT_FLOW1 = '5585'
ZMQ_VENT_HOST_FLOW1 = '10.128.55.67'
ZMQ_CTRL_HOST_FLOW1 = '10.128.55.67'

ZMQ_VENT_PORT_FLOW2 = '6388'
ZMQ_CTRL_VENT_PORT_FLOW2 = '5586'
ZMQ_VENT_HOST_FLOW2 = '10.128.55.67'
ZMQ_CTRL_HOST_FLOW2 = '10.128.55.67'

ZMQ_VENT_PORT_FLOW3 = '6389'
ZMQ_CTRL_VENT_PORT_FLOW3 = '5587'

ZMQ_VENT_PORT_FLOW4 = '6390'
ZMQ_CTRL_VENT_PORT_FLOW4 = '5588'

ZMQ_VENT_PORT_FLOW5 = '6391'
ZMQ_CTRL_VENT_PORT_FLOW5 = '5589'

# csv file path
BIN_FILE_PATH = '/home/ubuntu01/weibo'
REPLICA_BIN_FILE_PATH = '/home/ubuntu01/replica'
WRITTEN_TXT_PATH = '/home/ubuntu01/txt'

# first part of csv file1

FIRST_FILE_PART = 'MB_QL_9_7_NODE'

# sensitive words path
SENSITIVE_WORDS_PATH = '/home/ubuntu01/user_portrait/user_portrait/cron/flow4/sensitive_words.txt'

# need three ES identification 
USER_PROFILE_ES_HOST = ['10.128.55.81','10.128.55.82','10.128.55.83']
USER_PROFILE_ES_PORT = 9200
USER_PORTRAIT_ES_HOST = ['10.128.55.69', '10.128.55.70', '10.128.55.71']
USER_PORTRAIT_ES_PORT = 9200
FLOW_TEXT_ES_HOST = ['10.128.55.75', '10.128.55.76']
FLOW_TEXT_ES_PORT = 9200



# use to identify the db number of redis-97
R_BEGIN_TIME = '2016-03-21'

# use to recommentation
RECOMMENTATION_FILE_PATH = '/home/ubuntu01/user_portrait/recommentaion_file'
RECOMMENTATION_TOPK = 10000

# use to config leveldb
#DEFAULT_LEVELDBPATH = '/home/ubuntu8/huxiaoqian/user_portrait_leveldb'

# use to upload the user list for group task
UPLOAD_FOLDER = '/home/ubuntu01/user_portrait/cron/group/upload/'
ALLOWED_EXTENSIONS = set(['txt'])

# use to save user_portrait weibo 7day
#XAPIAN_DB_PATH = 'user_portrait_weibo'
#XAPIAN_DATA_DIR = '/home/ubuntu8/huxiaoqian/user_portrait_weibo_xapian/data/'
#XAPIAN_STUB_FILE_DIR = '/home/ubuntu8/huxiaoqian/user_portrait_weibo_xapian/stub/'

#XAPIAN_INDEX_SCHEMA_VERSION = 5
#XAPIAN_INDEX_LOCK_FILE = '/tmp/user_portrait_weibo_xapian'

#XAPIAN_SEARCH_DEFAULT_SCHEMA_VERSION = 5

#XAPIAN_ZMQ_POLL_TIMEOUT = 100000


# all weibo database
WEIBO_API_HOST = ''
WEIBO_API_PORT = ''


# redis/elasticsearch path
redis_path = '/home/redis-3.0.1'
es_path = '/home/elasticsearch-1.6.0'
