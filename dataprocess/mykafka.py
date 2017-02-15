# -*- coding: utf-8 -*-
import time
import MySQLdb
import ConfigParser
from pykafka import KafkaClient
from store import kafka_store

cf = ConfigParser.ConfigParser()
cf.read('/opt/app/fulllink/dataprocess/config.ini')
config = {}
config['host'] = cf.get('mysql', 'host') 
config['user'] = cf.get('mysql', 'user') 
config['passwd'] = cf.get('mysql', 'passwd')
config['db'] = cf.get('mysql', 'db') 

client = KafkaClient(hosts="pro-kafka1:9092, pro-kafka2:9092, pro-kafka3:9092")
topic = client.topics['http_request']
balanced_consumer = topic.get_balanced_consumer(
    consumer_group='fulllinkgroup',
    auto_commit_enable=True,
    zookeeper_connect='pro-kafka1:2181, pro-kafka2:2181, pro-kafka3:2181'
)

def addr2dec(addr): 
    items = [int(x) for x in addr.split(".")]  
    return sum([items[i] << [24, 16, 8, 0][i] for i in range(4)])  

def get_city(ip):
    try:    
        conn = MySQLdb.connect(host=config['host'],user=config['user'],passwd=config['passwd'],db=config['db'],port=3306,charset='utf8')
        cur = conn.cursor()
        if ip:
            num = addr2dec(ip)
            sql = 'select `city` from ip_city where {0}>ipbeginnum order by ipbeginnum desc limit 1;'.format(num)
            cur.execute(sql)
            return cur.fetchall()[0][0]
        else:
            return '*'
    except Exception as e:
        print e.message
        raise e 
    finally:
        cur.close()
        conn.close() 

def get_kafka():
    count = 0 
    for message in balanced_consumer:
        if message is not None:
            value = message.value.split('\001')
            if count % 100 == 0:
                count = 1 
                value = message.value.split('\001')
                d = {}
                for i in value:
                    if i:
                        d[i.split('=')[0]] = i.split('=')[1]
                if float(d.get('time'))/1000 < time.time()+60:
                    log_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(d.get('time'))/1000))
                    ip = d.get('ip')
                    os = d.get('os')
                    mobile_operator = d.get('mobile_operator')
                    action_param = d.get('action_param')
                    code = action_param.split(';')[0].split(':')[1]
                    resp_time = float(action_param.split(';')[1].split(':')[1])
                    nslookup = float(d.get('time'))/10000000
                    target = d.get('target').split('?')[0]
                    city = get_city(ip)
                    data = [log_time, ip, mobile_operator, code, resp_time, nslookup, target, city, os]
                    kafka_store(data)
                    print data
            else:
                count += 1
                continue

if __name__ == '__main__':
    get_kafka()
