#!/usr/bin/env python

import time
import json
from datetime import datetime
from kafka import KafkaConsumer
from time import localtime,strftime

LOG_PATH = "/mnt/logs/show_log"

hour = lambda : str(datetime.now().hour).zfill(2)
day = lambda : strftime("%Y-%m-%d", localtime())


def generateLog(value):
    try:
        h = hour()
        d = day()
        value = json.loads(value)
        if value.has_key('t'):
            t = time.localtime(float(value['t']))
            d = time.strftime('%Y-%m-%d', t)
            h = time.strftime('%H', t)
        user = unionid = executeid = creativeid = ''
        pid = areaid = advid = rid = exchange_price = bid_price = ''
        if value.has_key('userid'):
            user = value['userid']
        if value.has_key('unionid'):
            unionid = value['unionid']
        if value.has_key('executeid'):
            executeid = value['executeid']
        if value.has_key('creativeid'):
            creativeid = value['creativeid']
        if value.has_key('pid'):
            pid = value['pid']
        if value.has_key('areaid'):
            areaid = value['areaid']
        if value.has_key('advid'):
            advid = value['advid']
        if value.has_key('rid'):
            rid = value['rid']
        if value.has_key('bid_price'):
            bid_price = value['bid_price']
        if value.has_key('exchange_price'):
            exchange_price = value['exchange_price']
       
        msg = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (d, h, unionid, executeid, advid, pid, areaid, creativeid, user, rid, exchange_price, bid_price)
        return msg
    except Exception,e:
        print e
        return 


class LogFile():
    def __init__(self):
        self.m_hour = hour()
        self.fd = open( "%s/%s-%s.log" % (LOG_PATH, day(), hour()), 'a+')
        
    def getLogFile(self):
        try:
            h = hour()
            if h != self.m_hour:
                self.fd.close()
                self.m_hour = h
                self.fd = open( "%s/%s-%s.log" % (LOG_PATH, day(), hour()), 'a+')
                return self.fd
            else:
                return self.fd
        except Exception,e:
            print e


log = LogFile()
#'''
consumer = KafkaConsumer('mt-show-v1',#'topic2',
                           bootstrap_servers=['localhost:9092'],
                           group_id='my_consumer_group',
                           auto_commit_enable=True,
                           auto_commit_interval_ms=30 * 1000,
                           auto_offset_reset='smallest')

  # Infinite iteration
for message in consumer:
    #do_some_work(m)
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))
    fd = log.getLogFile()
    msg = generateLog(message.value)
    if msg:    
        fd.write(msg)
        fd.write('\n')
        fd.flush()
    # Mark this message as fully consumed
    # so it can be included in the next commit
    #
    # **messages that are not marked w/ task_done currently do not commit!
    consumer.task_done(message)

  # If auto_commit_enable is False, remember to commit() periodically
    consumer.commit()

  # Batch process interface
while True:
    for m in kafka.fetch_messages():
        process_message(m)
        consumer.task_done(m)
#'''
