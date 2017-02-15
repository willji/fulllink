# -*- coding: utf-8 -*-
import ConfigParser
from store import *
from crawl import *
from analysis import *
from tool import *
from celery import shared_task
from zabbix.api import ZabbixAPI

cf = ConfigParser.ConfigParser()
config = {}
cf.read('/opt/app/fulllink/dataprocess/config.ini')
config['fulllink_get_tree_url'] = cf.get('fulllink', 'get_tree_url') 
config['zabbix_url'] = cf.get('zabbix', 'url')
config['zabbix_user'] = cf.get('zabbix', 'user')
config['zabbix_password'] = cf.get('zabbix', 'password')

@shared_task
def get_all_domains():
    try:
        get_domains(log_type='netscaler', domain='ns_domain')
        get_nginx_domains(bool_value='must')
        get_nginx_domains(bool_value='must_not')
        get_domains(log_type='haproxy', domain='ha_domain')
    except Exception as e:
        print e

@shared_task
def run_es_ten_seconds():
    try:
        index = get_index()
        mytime = time.time()
        log_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mytime))
        from_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(mytime-28810))
        to_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(mytime-28800))
        
        data = get_netscaler_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='netscaler', interval=10, data=data, log_time=log_time)
        if not data:
            compensate.delay(index=index, log_type='netscaler', from_time=from_time, to_time=to_time, inerval=10, log_time=log_time)
        
        data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type='intranet')
        es_store(log_type='nginx', interval=10, data=data, log_time=log_time, nginx_type='intranet')
        if not data:
            compensate.delay(index=index, log_type='nginx', from_time=from_time, to_time=to_time, inerval=10, log_time=log_time, nginx_type='intranet')
        
        data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type='extranet')
        es_store(log_type='nginx', interval=10, data=data, log_time=log_time, nginx_type='extranet')
        if not data:
            compensate.delay(index=index, log_type='nginx', from_time=from_time, to_time=to_time, inerval=10, log_time=log_time, nginx_type='extranet')
        
        data = get_forward_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='nginx', interval=10, data=data, log_time=log_time, nginx_type='extranet')
        if not data:
            compensate.delay(index=index, log_type='nginx_forward', from_time=from_time, to_time=to_time, inerval=10, log_time=log_time, nginx_type='extranet')
        
        data = get_haproxy_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='haproxy', interval=10, data=data, log_time=log_time)
        if not data:
            compensate.delay(index=index, log_type='haproxy', from_time=from_time, to_time=to_time, inerval=10, log_time=log_time)
    except Exception as e:
        print e

@shared_task
def run_es_twenty_seconds():
    try:
        index = get_index('nginx')
        mytime = time.time()
        log_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mytime))
        from_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(mytime-28860))
        to_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(mytime-28800))
        
        data = get_netscaler_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='netscaler', interval=20, data=data, log_time=log_time)
        if not data:
            compensate.delay(index=index, log_type='netscaler', from_time=from_time, to_time=to_time, inerval=20, log_time=log_time)
        
        index = get_index('nginx')
        data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type='intranet')
        es_store(log_type='nginx', interval=20, data=data, log_time=log_time, nginx_type='intranet')
        if not data:
            compensate.delay(index=index, log_type='nginx', from_time=from_time, to_time=to_time, inerval=20, log_time=log_time, nginx_type='intranet')
        
        data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type='extranet')
        es_store(log_type='nginx', interval=20, data=data, log_time=log_time, nginx_type='extranet')
        if not data:
            compensate.delay(index=index, log_type='nginx', from_time=from_time, to_time=to_time, inerval=20, log_time=log_time, nginx_type='extranet')
        
        data = get_forward_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='nginx', interval=20, data=data, log_time=log_time, nginx_type='extranet')
        if not data:
            compensate.delay(index=index, log_type='nginx_forward', from_time=from_time, to_time=to_time, inerval=20, log_time=log_time, nginx_type='extranet')
        
        index = get_index('haproxy')
        data = get_haproxy_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='haproxy', interval=20, data=data, log_time=log_time)
        if not data:
            compensate.delay(index=index, log_type='haproxy', from_time=from_time, to_time=to_time, inerval=20, log_time=log_time)
    except Exception as e:
        print str(e)

@shared_task
def run_es_one_minuter():
    try:
        index = get_index()
        mytime = time.time()
        log_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mytime))
        from_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(mytime-28860))
        to_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(mytime-28800))
        
        data = get_netscaler_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='netscaler', interval=60, data=data, log_time=log_time)
        if not data:
            compensate.delay(index=index, log_type='netscaler', from_time=from_time, to_time=to_time, inerval=60, log_time=log_time)
        
        data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type='intranet')
        es_store(log_type='nginx', interval=60, data=data, log_time=log_time, nginx_type='intranet')
        if not data:
            compensate.delay(index=index, log_type='nginx', from_time=from_time, to_time=to_time, inerval=60, log_time=log_time, nginx_type='intranet')
        
        data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type='extranet')
        es_store(log_type='nginx', interval=60, data=data, log_time=log_time, nginx_type='extranet')
        if not data:
            compensate.delay(index=index, log_type='nginx', from_time=from_time, to_time=to_time, inerval=60, log_time=log_time, nginx_type='extranet')
        
        data = get_forward_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='nginx', interval=60, data=data, log_time=log_time, nginx_type='extranet')
        if not data:
            compensate.delay(index=index, log_type='nginx_forward', from_time=from_time, to_time=to_time, inerval=60, log_time=log_time, nginx_type='extranet')
        
        data = get_haproxy_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='haproxy', interval=60, data=data, log_time=log_time)
        if not data:
            compensate.delay(index=index, log_type='haproxy', from_time=from_time, to_time=to_time, inerval=60, log_time=log_time)
    except Exception as e:
        print e

@shared_task
def run_es_ten_minuters():
    try:
        index = get_index('nginx')
        mytime = time.time()
        log_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mytime))
        from_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(mytime-29400))
        to_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(mytime-28800))
        
        data = get_netscaler_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='netscaler', interval=600, data=data, log_time=log_time)
        if not data:
            compensate.delay(index=index, log_type='netscaler', from_time=from_time, to_time=to_time, inerval=600, log_time=log_time)
        
        index = get_index('nginx')
        data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type='intranet')
        es_store(log_type='nginx', interval=600, data=data, log_time=log_time, nginx_type='intranet')
        if not data:
            compensate.delay(index=index, log_type='nginx', from_time=from_time, to_time=to_time, inerval=600, log_time=log_time, nginx_type='intranet')
        
        data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type='extranet')
        es_store(log_type='nginx', interval=600, data=data, log_time=log_time, nginx_type='extranet')
        if not data:
            compensate.delay(index=index, log_type='nginx', from_time=from_time, to_time=to_time, inerval=600, log_time=log_time, nginx_type='extranet')
        
        data = get_forward_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='nginx', interval=600, data=data, log_time=log_time, nginx_type='extranet')
        if not data:
            compensate.delay(index=index, log_type='nginx_forward', from_time=from_time, to_time=to_time, inerval=600, log_time=log_time, nginx_type='extranet')
        
        index = get_index('haproxy')
        data = get_haproxy_data(index=index, from_time=from_time, to_time=to_time)
        es_store(log_type='haproxy', interval=600, data=data, log_time=log_time)
        if not data:
            compensate.delay(index=index, log_type='haproxy', from_time=from_time, to_time=to_time, inerval=600, log_time=log_time)
    except Exception as e:
        print e

@shared_task
def run_zabbix():
    try:
        domains = get_zabbixapp('app.ymatou.com')
        for domain in domains:
            try:
                log_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                data = get_zabbix(domain)
                zabbix_store(data=data, domain=domain, log_time=log_time)
            except:
                print(domain)
                continue
    except Exception as e:
        print e

@shared_task
def run_analysis():
    try:
        log_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        s = Solver()
        url = config['fulllink_get_tree_url']
        r = requests.post(url=url, data={'domain':'app.ymatou.com'})
        if json.loads(r.text)['retcode'] == 0:
            tree_data = json.loads(r.text)['stdout']
        else:
            raise Exception(json.loads(r.text)['stderr'])
        data = json.dumps(s.package(data=tree_data, app_name='app.ymatou.com'))
        analysis_store(data=data, domain='app.ymatou.com', log_time=log_time)
    except Exception as e:
        print e

@shared_task
def get_zabbix_host():
    try:
        d = {}
        domains = get_zabbixapp('app.ymatou.com')
        for domain in domains: 
            zapi = ZabbixAPI(url=config['zabbix_url'], user=config['zabbix_user'], password=config['zabbix_password'])
            cpuload = zapi.item.get(group=domain, output=['hostid','lastvalue'], search={'key_':'system.cpu.load[percpu,avg1]'})
            for i in cpuload:
                host = zapi.host.get(hostids=i.get('hostid'), output=['host'])[0]['host']
                d[i.get('hostid')] = host
        with open('/opt/app/fulllink/dataprocess/zabbix_host.txt', 'w') as f:
            for k,v in d.items():
                f.write(k+' '+v+'\n')
    except Exception as e:
        print e

@shared_task
def update_table():
    delete_last_week_table()
    change_this_week_table()
    create_this_week_table()

if __name__ == '__main__':
    update_table()
