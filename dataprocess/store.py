# -*- coding: utf-8 -*-
import MySQLdb 
import ConfigParser

cf = ConfigParser.ConfigParser()
cf.read('/opt/app/fulllink/dataprocess/config.ini')
config = {}
config['host'] = cf.get('mysql', 'host') 
config['user'] = cf.get('mysql', 'user') 
config['passwd'] = cf.get('mysql', 'passwd')
config['db'] = cf.get('mysql', 'db') 

def execute_sql(db, sql):
    try:    
        conn = MySQLdb.connect(host=config['host'],user=config['user'],passwd=config['passwd'],db=db,port=3306,charset='utf8')
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print e.message
        raise e 
    finally:
        cur.close()
        conn.close()

def kafka_store(data):
    db = config['db']
    log_time, ip, mobile_operator, code, resp_time, nslookup, target, city, os = data
    sql = "INSERT INTO `kafkalog`(`time`, `ip`, `mobile_operator`, `code`, `resp_time`, `nslookup`, `target`, `city`, `os`) VALUES('{0}', '{1}', '{2}', {3}, {4}, {5}, '{6}', '{7}', '{8}')"\
        .format(log_time, ip, mobile_operator, code, resp_time, nslookup, target, city, os)
    execute_sql(db=db, sql=sql)

def analysis_store(data, domain, log_time):
    db = config['db']
    sql = "INSERT INTO `analysis`(`time`, `domain`, `data`) VALUES('{0}', '{1}', '{2}')".format(log_time, domain, data)
    execute_sql(db=db, sql=sql)

def zabbix_store(data, domain, log_time):
    db = config['db']
    for d in data:
        host = d.get('host')
        connections = d.get('connections')
        cpuload = d.get('cpuload')
        if connections and cpuload:
            sql = "INSERT INTO `zabbixlog`(`time`, `domain`, `host`, `connections`, `cpuload`) \
                VALUES('{0}', '{1}', '{2}', {3}, {4})".format(log_time, domain, host, connections, cpuload)
        elif connections and not cpuload:
            sql = "INSERT INTO `zabbixlog`(`time`, `domain`, `host`, `connections`) \
                VALUES('{0}', '{1}', '{2}', {3})".format(log_time, domain, host, connections)
        elif not connections and cpuload:
            sql = "INSERT INTO `zabbixlog`(`time`, `domain`, `host`, `cpuload`) \
                VALUES('{0}', '{1}', '{2}', {3})".format(log_time, domain, host, cpuload)
        else:
            sql = "INSERT INTO `zabbixlog`(`time`, `domain`, `host`) \
                VALUES('{0}', '{1}', '{2}')".format(log_time, domain, host)
        execute_sql(db=db, sql=sql)

def es_store(log_type, interval, data, log_time, nginx_type=None, flag=None):
    db = config['db']
    if log_type == 'nginx':
        table = nginx_type + '_' + log_type + 'log_' + str(interval) + 's'
    else:   
        table = log_type + 'log_' + str(interval) + 's'
    if data:
        for d in data:
            if not flag:
                flag=0
            if log_type == 'netscaler':
                value = [d.get('domain'),d.get('average_server_time'),d.get('2XX',0),d.get('3XX',0),d.get('4XX',0),d.get('5XX',0)]
                sql = "INSERT INTO `{0}`(`time`, `domain`, `average_server_time`, `2XX`, `3XX`, `4XX`, `5XX`, `flag`) \
                    VALUES('{1}', '{2}', {3}, {4}, {5}, {6}, {7}, {8})".format(table, log_time, value[0], \
                    value[1], value[2], value[3], value[4], value[5], flag)
                execute_sql(db=db, sql=sql)
            else:   
                if len(d) == 1:
                    domain = d.get('domain')
                    sql = "INSERT INTO `{0}`(`time`, `domain`, `flag`) VALUES('{1}', '{2}', {3})".format(table, log_time, domain, flag)
                    execute_sql(db=db, sql=sql)
                else:
                    value = [d.get('domain'),d.get('average_request_time'),d.get('average_response_time'),d.get('2XX',0),d.get('3XX',0),d.get('4XX',0),d.get('5XX',0)]
                    sql = "INSERT INTO `{0}`(`time`, `domain`, `average_request_time`, `average_response_time`, `2XX`, `3XX`, `4XX`, `5XX`, `flag`) \
                        VALUES('{1}', '{2}', {3}, {4}, {5}, {6}, {7}, {8}, {9})".format(table, log_time, value[0], \
                        value[1], value[2], value[3], value[4], value[5], value[6], flag)
                    execute_sql(db=db, sql=sql)
    else:
        if log_type == 'netscaler':
            filename = 'domains_netscaler.txt'
        elif log_type == 'nginx':
            if nginx_type == 'extranet':
                filename = 'domains_extranet_nginx.txt'
            elif nginx_type == 'intranet':
                filename = 'domains_intranet_nginx.txt'
        elif log_type == 'haproxy':
            filename = 'domains_haproxy.txt'
        with open('/opt/app/fulllink/dataprocess/files/{0}'.format(filename)) as f:    
            f = f.readlines()
        f = [i.rstrip() for i in f]
        for domain in f:
            sql = "INSERT INTO `{0}`(`time`, `domain`, `flag`) VALUES('{1}', '{2}', {3})".format(table, log_time, domain, 1)
            execute_sql(db=db, sql=sql)

def event_store(log_time, type, parent, app, info, level):
    db = config['db']
    sql = "INSERT INTO `event`(`time`, `type`, `parent`, `app`, `info`, `level`) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', {5})"\
        .format(log_time, type, parent, app, info, level)
    execute_sql(db=db, sql=sql)
