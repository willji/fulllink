# -*- coding: utf-8 -*-
import time
import ConfigParser
from store import es_store
from tool import get_forward, get_mainapps, get_jumpapps, get_apps
from celery import shared_task
from zabbix.api import ZabbixAPI
from elasticsearch import Elasticsearch

cf = ConfigParser.ConfigParser()
cf.read('/opt/app/fulllink/dataprocess/config.ini')
config = {}
config['es_host1'] = cf.get('es', 'host1')
config['es_host2'] = cf.get('es', 'host2')
config['es_host3'] = cf.get('es', 'host3')
config['es_port'] = cf.get('es', 'port')
config['zabbix_url'] = cf.get('zabbix', 'url')
config['zabbix_user'] = cf.get('zabbix', 'user')
config['zabbix_password'] = cf.get('zabbix', 'password')

es = Elasticsearch(hosts=[{'host': config['es_host1'], 'port': config['es_port']}, {'host': config['es_host2'], 'port': config['es_port']}, {'host': config['es_host3'], 'port': config['es_port']}])

def get_index(type):
    ISOTIMEFORMAT='%Y.%m.%d'
    return type + '-' + time.strftime(ISOTIMEFORMAT, time.localtime(time.time()-28800))

def get_domains(log_type, domain):
    doc_type = log_type + '-log'
    filename = 'domains_' + log_type + '.txt'
    index = 'logstash-' + time.strftime("%Y.%m.%d", time.localtime(time.time()-86400))
    res = es.search(index=index, doc_type=doc_type, body=
{
   "size" : 0,
   "aggs":{
        "domains": {
            "terms" : { "size": 0, "field" : domain }
        }
    }
}
)
    with open('/opt/app/fulllink/dataprocess/files/{0}'.format(filename), 'w') as f:    
        for i in res['aggregations']['domains']['buckets']:
            f.write(i['key']+'\n')

def get_nginx_domains(bool_value):
    if bool_value == 'must':
        filename = 'domains_extranet_nginx.txt'
    elif bool_value == 'must_not':
        filename = 'domains_intranet_nginx.txt'
    index = 'logstash-' + time.strftime("%Y.%m.%d", time.localtime(time.time()-86400))
    res = es.search(index=index, doc_type='nginx-log', body=
{
    "size" : 0,
    "query":{
    "bool": {
        bool_value:[
            {"match": {"client_ip": "127.0.0.1"}}
        ]   
        }   
    },  
    "aggs":{
        "domains": {
            "terms" : { "size": 1000, "field" : 'domain' }
        }
    }
}
)
    with open('/opt/app/fulllink/dataprocess/files/{0}'.format(filename), 'w') as f:    
        for i in res['aggregations']['domains']['buckets']:
            f.write(i['key']+'\n')

@shared_task
def compensate(index, log_type, from_time, to_time, inerval, log_time, nginx_type=None):
    sleep_time = 60
    for i in range(10):
        time.sleep(sleep_time)
        if log_type == 'netscaler':
            data = get_netscaler_data(index=index, from_time=from_time, to_time=to_time)
            if not data:
                if sleep_time < 480:
                    sleep_time = sleep_time * 2
            else:
                es_store(log_type='netscaler', interval=inerval, data=data, log_time=log_time, flag=2)
                return
        elif log_type == 'haproxy':
            data = get_haproxy_data(index=index, from_time=from_time, to_time=to_time)
            if not data:
                if sleep_time < 480:
                    sleep_time = sleep_time * 2
            else:
                es_store(log_type='haproxy', interval=inerval, data=data, log_time=log_time, flag=2)
                return
        elif log_type == 'nginx':
            data = get_nginx_data(index=index, from_time=from_time, to_time=to_time, nginx_type=nginx_type)
            if not data:
                if sleep_time < 480:
                    sleep_time = sleep_time * 2
            else:
                es_store(log_type='nginx', interval=inerval, data=data, log_time=log_time, nginx_type=nginx_type, flag=2)
                return
        elif log_type == 'nginx_forward':
            data = get_forward_data(index=index, from_time=from_time, to_time=to_time)
            if not data:
                if sleep_time < 480:
                    sleep_time = sleep_time * 2
            else:
                es_store(log_type='nginx', interval=inerval, data=data, log_time=log_time, nginx_type='extranet', flag=2)
                return

def get_nginx_code(index, bool_value, from_time, to_time):
    res = es.search(index=index, doc_type='nginx-log', body=
{
    "size" : 0,
    "query":{
    "bool": {
        bool_value:[
            {"match": {"client_ip": "127.0.0.1"}}
        ]   
        }   
    },  
    "aggs":{
        "recent_time": {
            "filter": { 
                "range": {
                    "@timestamp": {
                        "from": from_time, "to" :to_time
                    }   
                }   
            },  
    "aggs": {
        "group_by_code": {
            "range": {
                "field": 'response',
                "ranges": [
                {   
                    "key": "2XX",
                    "from": 200,
                    "to": 300 
                },
                {
                    "key": "3XX",
                    "from": 300,
                    "to": 400
                },
                {
                    "key": "4XX",
                    "from": 400,
                    "to": 500
                },
                {
                    "key": "5XX",
                    "from": 500,
                    "to": 600
                }
                ]
            },
            "aggs": {
                "group_by_domain": {
                    "terms": {
                        "size" : 1000, "field": 'domain'
                    }
                }
            }
        }
        }
        }
    }
}
)
    return res['aggregations']['recent_time']['group_by_code']['buckets']

def get_nginx_time(index, bool_value, from_time, to_time):
    res = es.search(index=index, doc_type='nginx-log', body=
{
   "size" : 0,
   "query":{
    "bool": {
       bool_value:[
        {"match": {"client_ip": "127.0.0.1"}}
    ]   
    }   
   },  
   "aggs":{
      "recent_time": {
         "filter": { 
            "range": {
               "@timestamp": {
                  "from": from_time, "to" :to_time
               }   
            }   
         },  
        "aggs" : { 
            "group_by_domain": {
                "terms" : { 
                    "size" : 1000, "field" : 'domain'
                },  
                "aggs" : { 
                    "average_request_time" : { 
                        "avg" : { 
                            "field": 'request_time'
                        }   
                    },  
                    "average_response_time" : { 
                        "avg" : { 
                            "field": 'upstream_response_time'
                        }
                    }
                }
            }
        }
      }
   }
}
)
    return res['aggregations']['recent_time']['group_by_domain']['buckets']

def get_forward_code(index, from_time, to_time):
    res = es.search(index=index, doc_type='nginx-log', body=
{
    "size" : 10,
    "query":{
    "bool": {
        "must":[
            {"match": {"client_ip": "127.0.0.1"}},
            {"match": {"domain": "app.ymatou.com"}}
        ]   
        }   
    },  
    "aggs":{
        "recent_time": {
            "filter": { 
                "range": {
                    "@timestamp": {
                        "from": from_time, "to" :to_time
                    }   
                }   
            },  
    "aggs": {
        "group_by_code": {
            "range": {
                "field": 'response',
                "ranges": [
                {   
                    "key": "2XX",
                    "from": 200,
                    "to": 300 
                },
                {
                    "key": "3XX",
                    "from": 300,
                    "to": 400
                },
                {
                    "key": "4XX",
                    "from": 400,
                    "to": 500
                },
                {
                    "key": "5XX",
                    "from": 500,
                    "to": 600
                }
                ]
            },
            "aggs": {
                "group_by_request": {
                    "terms": {
                        "size" : 10000, "field": 'request'
                    }
                }
            }
        }
        }
        }
    }
}
)
    return res['aggregations']['recent_time']['group_by_code']['buckets']

def get_forward_time(index, from_time, to_time):
    res = es.search(index=index, doc_type='nginx-log', body=
{
   "size" : 0,
   "query":{
    "bool": {
       "must":[
        {"match": {"client_ip": "127.0.0.1"}},
        {"match": {"domain": "app.ymatou.com"}}
    ]   
    }   
   },  
   "aggs":{
      "recent_time": {
         "filter": { 
            "range": {
               "@timestamp": {
                  "from": from_time, "to" :to_time
               }   
            }   
         },  
        "aggs" : { 
            "group_by_request": {
                "terms" : { 
                    "size" : 1000, "field" : 'request'
                },  
                "aggs" : { 
                    "average_request_time" : { 
                        "avg" : { 
                            "field": 'request_time'
                        }   
                    },  
                    "average_response_time" : { 
                        "avg" : { 
                            "field": 'upstream_response_time'
                        }
                    }
                }
            }
        }
      }
   }
}
)
    return res['aggregations']['recent_time']['group_by_request']['buckets']

def get_netscaler_code(index, from_time, to_time):
    res = es.search(index=index, doc_type='netscaler-log', body=
{
    "query": {
        "range" : { 
            "@timestamp" : { 
                "from" : from_time, "to" : to_time
            } 
        }
    },
    "size" : 0,
    "aggs": {
        "group_by_code": {
            "range": {
                "field": 'ns_response',
                "ranges": [
                {
                    "key": "2XX",
                    "from": 200,
                    "to": 300
                },
                {
                    "key": "3XX",
                    "from": 300,
                    "to": 400
                },
                {
                    "key": "4XX",
                    "from": 400,
                    "to": 500
                },
                {
                    "key": "5XX",
                    "from": 500,
                    "to": 600
                }
                ]
            },
            "aggs": {
                "group_by_domain": {
                    "terms": {
                        "size" : 1000, "field": 'ns_domain'
                    }
                }
            }
        }
    }
}
)
    return res['aggregations']['group_by_code']['buckets']

def get_netscaler_time(index, from_time, to_time):
    res = es.search(index=index, doc_type='netscaler-log', body=
{
    "query": {
        "range" : { 
            "@timestamp" : { 
                "from" : from_time, "to" : to_time 
            } 
        }
    },
    "size" : 0,
    "aggs" : {
        "group_by_domain": {
            "terms" : {
                "size" : 1000, "field" : "ns_domain"
            },
            "aggs" : {
                "average_server_time" : {
                    "avg" : {
                        "field":"ns_servertime" 
                    }
                }
            }
        }
    }
}
)
    return res['aggregations']['group_by_domain']['buckets']

def get_haproxy_time(index, from_time, to_time):
    res = es.search(index=index, doc_type='haproxy-log', body=
{
    "query": {
        "range" : { 
            "@timestamp" : { 
                "from" : from_time, "to" : to_time
            } 
        }
    },
    "size" : 0,
    "aggs" : {
        "group_by_domain": {
            "terms" : {
               "size" : 1000, "field" : 'ha_domain'
            },
            "aggs" : {
                "average_request_time" : {
                    "avg" : {
                        "field": "ha_requesttime"  
                    }
                },
                "average_response_time" : {
                    "avg" : {
                        "field": "ha_responsetime"
                    }
                }
            }
        }
    }
}
)
    return res['aggregations']['group_by_domain']['buckets']

def get_haproxy_code(index, from_time, to_time):
    res = es.search(index=index, doc_type='haproxy-log', body=
{
    "query": {
        "range" : { 
            "@timestamp" : { 
                "from" : from_time, "to" : to_time
            } 
        }
    },
    "size" : 0,
    "aggs": {
        "group_by_code": {
            "range": {
                "field": 'ha_response',
                "ranges": [
                {
                    "key": "2XX",
                    "from": 200,
                    "to": 300
                },
                {
                    "key": "3XX",
                    "from": 300,
                    "to": 400
                },
                {
                    "key": "4XX",
                    "from": 400,
                    "to": 500
                },
                {
                    "key": "5XX",
                    "from": 500,
                    "to": 600
                }
                ]
            },
            "aggs": {
                "group_by_domain": {
                    "terms": {
                        "size" : 1000, "field": 'ha_domain'
                    }
                }
            }
        }
    }
}
)
    return res['aggregations']['group_by_code']['buckets']

def data_convergence(tmp_time_data, tmp_code_data, log_type=None):
    data = []
    time_data = {}
    code_data = {}
    for i in tmp_time_data:
        if log_type == 'netscaler':
            time_data[i['key']] = {'average_server_time':i['average_server_time']['value']}
        else:
            time_data[i['key']] = {'average_request_time':i['average_request_time']['value'], 'average_response_time':i['average_response_time']['value']}
    for i in tmp_code_data:
        for j in i['group_by_domain']['buckets']:
            if code_data.has_key(j['key']):
                code_data[j['key']][i['key']] = j['doc_count']
            else:
                code_data[j['key']] = {i['key']:j['doc_count']}
    for i in time_data:
        if log_type == 'netscaler':
            tmp_data = {'domain':i, 'average_server_time':time_data[i]['average_server_time']}
        else:
            tmp_data = {'domain':i, 'average_request_time':time_data[i]['average_request_time'], 'average_response_time':time_data[i]['average_response_time']}
        if code_data.has_key(i):
            tmp_data['2XX'] = code_data[i].get('2XX', 0)
            tmp_data['3XX'] = code_data[i].get('3XX', 0)
            tmp_data['4XX'] = code_data[i].get('4XX', 0)
            tmp_data['5XX'] = code_data[i].get('5XX', 0)
        data.append(tmp_data)
    return data

def get_netscaler_data(index, from_time, to_time):
    return [{'domain': u'app.ymatou.com', 'average_server_time': 0, '5XX': 0, '2XX': 0, '4XX': 0, '3XX': 0}]
    tmp_netscaler_time_data = get_netscaler_time(index, from_time=from_time, to_time=to_time)
    tmp_netscaler_code_data = get_netscaler_code(index, from_time=from_time, to_time=to_time)
    log_data = data_convergence(tmp_time_data = tmp_netscaler_time_data, tmp_code_data = tmp_netscaler_code_data, log_type='netscaler')
    for d in log_data:
        if not d.get('average_server_time'):
            d['average_server_time'] = 0
        else:
            d['average_server_time'] = round(d['average_server_time'], 3)
    tmp_apps = [i['domain'] for i in log_data]
    log_data = []
    tmp_apps = []
    apps = get_mainapps()
    for i in apps:
        if i not in tmp_apps:
            log_data.append({'domain': i})
    return log_data

def get_nginx_data(index, from_time, to_time, nginx_type):
    if nginx_type == 'intranet':
        tmp_nginx_time_data = get_nginx_time(index, bool_value='must_not', from_time=from_time, to_time=to_time)
        tmp_nginx_code_data = get_nginx_code(index, bool_value='must_not', from_time=from_time, to_time=to_time)
    else:
        tmp_nginx_time_data = get_nginx_time(index, bool_value='must', from_time=from_time, to_time=to_time)
        tmp_nginx_code_data = get_nginx_code(index, bool_value='must', from_time=from_time, to_time=to_time)
    log_data = data_convergence(tmp_time_data = tmp_nginx_time_data, tmp_code_data = tmp_nginx_code_data)
    for d in log_data:
        if not d.get('average_request_time'):
            d['average_request_time'] = 0
        else:
            d['average_request_time'] = round(d['average_request_time'], 3)
        if not d.get('average_response_time'):
            d['average_response_time'] = 0
        else:
            d['average_response_time'] = round(d['average_response_time'], 3)
    if nginx_type == 'extranet':
        tmp_apps = [i['domain'] for i in log_data]
        apps = get_mainapps()
        for i in apps:
            if i not in tmp_apps:
                log_data.append({'domain': i})
    else:
        tmp_apps = [i['domain'] for i in log_data]
        apps = get_apps()
        for i in apps:
            if i not in tmp_apps:
                log_data.append({'domain': i})
    return log_data

def get_haproxy_data(index,from_time, to_time):
    tmp_haproxy_time_data = get_haproxy_time(index, from_time=from_time, to_time=to_time)
    tmp_haproxy_code_data = get_haproxy_code(index, from_time=from_time, to_time=to_time)
    log_data = data_convergence(tmp_time_data = tmp_haproxy_time_data, tmp_code_data = tmp_haproxy_code_data)
    for d in log_data:
        if not d.get('average_request_time'):
            d['average_request_time'] = 0
        else:
            d['average_request_time'] = round(d['average_request_time']/1000.0, 3)
        if not d.get('average_response_time'):
            d['average_response_time'] = 0
        else:
            d['average_response_time'] = round(d['average_response_time']/1000.0, 3)
    tmp_apps = [i['domain'] for i in log_data]
    apps = get_apps()
    for i in apps:
        if i not in tmp_apps:
            log_data.append({'domain': i})
    return log_data

def replace_time_data(forward_time):
    forward_table = get_forward('app.ymatou.com')
    tmp_data = []
    for k in forward_table:
        for i in forward_time:
            if k in i['key']:
                tmp_data.append({'domain':forward_table[k], 'count':i['doc_count'], 'average_request_time':i['average_request_time']['value'], 'average_response_time':i['average_response_time']['value']})
    data = {}
    for i in tmp_data:
        if data.has_key(i['domain']):
            count = i.get('count', 0) + data[i['domain']].get('count', 0)
            average_request_time = i.get('average_request_time', 0)*i.get('count', 0) + data[i['domain']].get('average_request_time', 0)
            average_response_time = i.get('average_response_time', 0)*i.get('count', 0) + data[i['domain']].get('average_response_time', 0)
            data[i['domain']] = {'average_request_time':average_request_time,'average_response_time':average_response_time,'count':count}
        else:
            data[i['domain']] = {'average_request_time':i.get('average_request_time', 0)*i.get('count', 0),'average_response_time':i.get('average_response_time', 0)*i.get('count', 0),'count':i.get('count', 0)}
    for k in data:
        count = data[k]['count']
        average_request_time = round(data[k]['average_request_time'] / count, 3)
        average_response_time = round(data[k]['average_response_time'] / count, 3)
        data[k] = {'count':count, 'average_request_time':average_request_time, 'average_response_time':average_response_time}
    return data

def replace_code_data(forward_code):
    forward_table = get_forward('app.ymatou.com')
    tmp_data = []
    for k in forward_table:
        for j in range(4):
            for i in forward_code[j]['group_by_request']['buckets']:
                code = str(j+2) + 'XX'
                if k in i['key']:
                    tmp_data.append({'domain':forward_table[k], code:i['doc_count']})
    data = {}
    for i in tmp_data:
        if data.has_key(i['domain']):
            XX2 = i.get('2XX', 0) + data[i['domain']].get('2XX', 0)
            XX3 = i.get('3XX', 0) + data[i['domain']].get('3XX', 0)
            XX4 = i.get('4XX', 0) + data[i['domain']].get('4XX', 0) 
            XX5 = i.get('5XX', 0) + data[i['domain']].get('5XX', 0) 
            data[i['domain']] = {'2XX':XX2,'3XX':XX3,'4XX':XX4,'5XX':XX5}
        else:
            data[i['domain']] = {'2XX':i.get('2XX', 0),'3XX':i.get('3XX', 0),'4XX':i.get('4XX', 0),'5XX':i.get('5XX', 0)}
    return data

def get_forward_data(index, from_time, to_time):
    forward_code = get_forward_code(index=index, from_time=from_time, to_time=to_time)
    forward_time = get_forward_time(index=index, from_time=from_time, to_time=to_time)
    code_data = replace_code_data(forward_code)
    time_data = replace_time_data(forward_time)
    log_data = []
    for i in time_data:
        tmp_data = {'domain':i, 'average_request_time':time_data[i]['average_request_time'], 'average_response_time':time_data[i]['average_response_time']}
        if code_data.has_key(i):
            tmp_data['2XX'] = code_data[i].get('2XX', 0)
            tmp_data['3XX'] = code_data[i].get('3XX', 0)
            tmp_data['4XX'] = code_data[i].get('4XX', 0)
            tmp_data['5XX'] = code_data[i].get('5XX', 0)
        log_data.append(tmp_data)
    tmp_apps = [i['domain'] for i in log_data]
    apps = get_jumpapps()
    for i in apps:
        if i not in tmp_apps:
            log_data.append({'domain': i})
    return log_data

def get_zabbix(domain):
    d = {}
    with open('/opt/app/fulllink/dataprocess/zabbix_host.txt') as f:
        f = f.readlines()
        f = [[i.split(' ')[0], i.split(' ')[1].rstrip('\n')] for i in f]
        for i in f:
            d[i[0]] = i[1]
    zapi = ZabbixAPI(url=config['zabbix_url'], user=config['zabbix_user'], password=config['zabbix_password'])
    connections = zapi.item.get(group=domain, output=['hostid','lastvalue'], search={'key_':'perf_counter["\Web Service(_Total)\Current Connections",300]'})
    cpuload = zapi.item.get(group=domain, output=['hostid','lastvalue'], search={'key_':'perf_counter[\\Processor(_Total)\\% Processor Time]'})
    result = []
    if cpuload:
        for i in cpuload:
            result.append(dict(hostid=i.get('hostid'), cpuload=i.get('lastvalue')))
    else:
        cpuload = zapi.item.get(group=domain, output=['hostid','lastvalue'], search={'key_':'system.cpu.load[percpu,avg1]'}) 
        for i in cpuload:
            result.append(dict(hostid=i.get('hostid'), cpuload=float(i.get('lastvalue', 0))*100))
    for i in result: 
        for j in connections:
            if j.get('hostid') == i.get('hostid'):
                i['connections'] = j.get('lastvalue')
    for i in result: 
        i['host'] = d[i['hostid']]
    return result

if __name__ == '__main__':
    #data = get_forward_data('nginx-2016.12.15', 'now-1m', 'now')
    data = get_nginx_code('nginx-2016.12.15', bool_value='must_not', from_time='now-5m', to_time='now')
    #data = get_nginx_data(index='nginx-2016.12.15', from_time='now-1m', to_time='now', nginx_type='extranet')
    #data = get_netscaler_data(index='logstash-2016.12.06', from_time='now-1m', to_time='now')
    #data = get_netscaler_time(index='logstash-2016.12.06', from_time='now-10m', to_time='now')
    #data = get_haproxy_time(index='haproxy-2016.12.15', from_time='now-1m', to_time='now')
    #data = get_haproxy_data(index='haproxy-2016.12.15', from_time='now-1m', to_time='now')
    #data = get_zabbix('ordercenter.app.ymatou.com')
    #data = get_netscaler_code(index='logstash-2016.11.04', from_time='now-1m', to_time='now')
    print data
