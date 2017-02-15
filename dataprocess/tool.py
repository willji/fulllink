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

def get_mysql_result(db, sql):
    try:    
        conn = MySQLdb.connect(host=config['host'],user=config['user'],passwd=config['passwd'],db=db,port=3306,charset='utf8')
        cur = conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        return data
    except Exception as e:
        print e.message
        raise e 
    finally:
        cur.close()
        conn.close()

def get_forward(domain):
    db = config['db']
    sql = "select `path`,`app` from `api_forward` where `domain`='{0}';".format(domain)
    r = get_mysql_result(db=db, sql=sql)
    data = {}
    for i in r:
        data[i[0]] = i[1]
    return data

def get_mainapps():
    db = config['db']
    sql = "select `name` from `api_dependent` where `type`='mainApp';"
    r = get_mysql_result(db=db, sql=sql)
    data = []
    for i in r:
        data.append(i[0])
    data = list(set(data))
    return data

def get_jumpapps():
    db = config['db']
    sql = "select `name` from `api_dependent` where `type`='jumpApp';"
    r = get_mysql_result(db=db, sql=sql)
    data = []
    for i in r:
        data.append(i[0])
    data = list(set(data))
    return data

def get_apps():
    db = config['db']
    sql = "select `name` from `api_dependent` where `type`='app';"
    r = get_mysql_result(db=db, sql=sql)
    data = []
    for i in r:
        data.append(i[0])
    data = list(set(data))
    return data

def get_zabbixapp(domain):
    db = config['db']
    sql = "select name from api_dependent where domain='{0}' and type='app' or type='jumpApp' or type='mainApp';".format(domain)
    data = get_mysql_result(db=db, sql=sql)
    if data:
        r = []  
        for i in data:
            r.append(i[0])
        r = list(set(r))
        return r
    else:   
        return []

def delete_last_week_table():
    try:    
        conn = MySQLdb.connect(host=config['host'],user=config['user'],passwd=config['passwd'],db=config['db'],port=3306,charset='utf8')
        cur = conn.cursor()
        cur.execute('drop table old_netscalerlog_20s')
        cur.execute('drop table old_netscalerlog_600s')
        cur.execute('drop table old_intranet_nginxlog_20s')
        cur.execute('drop table old_intranet_nginxlog_600s')
        cur.execute('drop table old_extranet_nginxlog_20s')
        cur.execute('drop table old_extranet_nginxlog_600s')
        cur.execute('drop table old_haproxylog_20s')
        cur.execute('drop table old_haproxylog_600s')
        cur.execute('drop table old_zabbixlog')
        cur.execute('drop table old_event')
        cur.execute('drop table old_analysis')
        cur.execute('drop table old_kafkalog')
        conn.commit()
    except Exception as e:
        print e.message
        raise e 
    finally:
        cur.close()
        conn.close()

def change_this_week_table():
    try:    
        conn = MySQLdb.connect(host=config['host'],user=config['user'],passwd=config['passwd'],db=config['db'],port=3306,charset='utf8')
        cur = conn.cursor()
        cur.execute('alter table netscalerlog_20s rename to old_netscalerlog_20s')
        cur.execute('alter table netscalerlog_600s rename to old_netscalerlog_600s')
        cur.execute('alter table intranet_nginxlog_20s rename to old_intranet_nginxlog_20s')
        cur.execute('alter table intranet_nginxlog_600s rename to old_intranet_nginxlog_600s')
        cur.execute('alter table extranet_nginxlog_20s rename to old_extranet_nginxlog_20s')
        cur.execute('alter table extranet_nginxlog_600s rename to old_extranet_nginxlog_600s')
        cur.execute('alter table haproxylog_20s rename to old_haproxylog_20s')
        cur.execute('alter table haproxylog_600s rename to old_haproxylog_600s')
        cur.execute('alter table zabbixlog rename to old_zabbixlog')
        cur.execute('alter table kafkalog rename to old_kafkalog')
        cur.execute('alter table event rename to old_event')
        cur.execute('alter table analysis rename to old_analysis')
        conn.commit()
    except Exception as e:
        print e.message
        raise e 
    finally:
        cur.close()
        conn.close()

def create_this_week_table():
    try:    
        conn = MySQLdb.connect(host=config['host'],user=config['user'],passwd=config['passwd'],db=config['db'],port=3306,charset='utf8')
        cur = conn.cursor()
        sql = '''
            CREATE TABLE `{0}`(
                `id` INT(11) NOT NULL AUTO_INCREMENT,
                `time` VARCHAR(50) NOT NULL,
                `domain` VARCHAR(80) NOT NULL,
                `average_server_time` float(11,3),
                `2XX` INT(11),
                `3XX` INT(11),
                `4XX` INT(11),
                `5XX` INT(11),
                `flag` INT(1),
                PRIMARY KEY (`id`),
                INDEX (`domain`,`flag`)
            )
            ;
              '''
        cur.execute(sql.format('netscalerlog_20s'))
        cur.execute(sql.format('netscalerlog_600s'))
        sql = '''
            CREATE TABLE `{0}`(
                `id` INT(11) NOT NULL AUTO_INCREMENT,
                `time` VARCHAR(50) NOT NULL,
                `domain` VARCHAR(80) NOT NULL,
                `average_request_time` float(11,3),
                `average_response_time` float(11,3),
                `2XX` INT(11),
                `3XX` INT(11),
                `4XX` INT(11),
                `5XX` INT(11),
                `flag` INT(1),
                PRIMARY KEY (`id`),
                INDEX (`domain`,`flag`)
            )
            ;
              '''
        cur.execute(sql.format('haproxylog_20s'))
        cur.execute(sql.format('haproxylog_600s'))
        cur.execute(sql.format('extranet_nginxlog_20s'))
        cur.execute(sql.format('extranet_nginxlog_600s'))
        cur.execute(sql.format('intranet_nginxlog_20s'))
        cur.execute(sql.format('intranet_nginxlog_600s'))
        sql = '''
            CREATE TABLE `zabbixlog`(
                `id` INT(11) NOT NULL AUTO_INCREMENT,
                `time` VARCHAR(50) NOT NULL,
                `domain` VARCHAR(80) NOT NULL,
                `host` VARCHAR(20) NOT NULL,
                `connections` float(11,4),
                `cpuload` float(11,4),
                PRIMARY KEY (`id`),
                INDEX (`domain`, `time`)
            )
            ; 
              '''
        cur.execute(sql)
        sql = '''  
            CREATE TABLE `kafkalog`(
                `id` INT(11) NOT NULL AUTO_INCREMENT,
                `time` VARCHAR(30) NOT NULL,
                `ip` VARCHAR(30),
                `mobile_operator` VARCHAR(20),
                `code` INT(11),
                `resp_time` float(11,3),
                `nslookup` float(11,3),
                `target` VARCHAR(100),
                `city` VARCHAR(50),
                `os` VARCHAR(20),
                PRIMARY KEY (`id`),
                INDEX (`time`)
            )
            ;
              '''
        cur.execute(sql)
        sql = '''
            CREATE TABLE `event`(
                `id` INT(11) NOT NULL AUTO_INCREMENT,
                `time` VARCHAR(30) NOT NULL,
                `type` VARCHAR(20) NOT NULL,
                `parent` VARCHAR(20) NOT NULL,
                `app` VARCHAR(50) NOT NULL,
                `info` TEXT NOT NULL,
                `level` INT(5) NOT NULL,
                PRIMARY KEY (`id`),
                INDEX (`time`)
            )
            ;
              '''
        cur.execute(sql)
        sql = '''
            CREATE TABLE `analysis`(
                `id` INT(11) NOT NULL AUTO_INCREMENT,
                `time` VARCHAR(50) NOT NULL,
                `domain` VARCHAR(50) NOT NULL,
                `data` TEXT NOT NULL,
                PRIMARY KEY (`id`),
                INDEX (`domain`)
            )
            ;
              '''
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print e.message
        raise e 
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    print get_mainapps()
    print get_jumpapps()
    print get_apps()
