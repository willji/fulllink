# -*- coding: utf-8 -*-
import json
import time
import tree
import MySQLdb
import ConfigParser
from django.shortcuts import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from dataprocess.tool import get_forward

cf = ConfigParser.ConfigParser()
cf.read('/opt/app/fulllink/dataprocess/config.ini')
config = {}
config['host'] = cf.get('mysql', 'host') 
config['user'] = cf.get('mysql', 'user') 
config['passwd'] = cf.get('mysql', 'passwd')
config['db'] = cf.get('mysql', 'db') 

def format_response(retcode, result):
    if retcode == 0:
        return HttpResponse(json.dumps({'retcode': retcode, 'stdout': result, 'stderr': None}))
    else:
        return HttpResponse(json.dumps({'retcode': retcode, 'stdout': None, 'stderr': result}))

def get_mysql_result(sql):
    try:
        conn = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['passwd'], db=config['db'], port=3306, charset='utf8')
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

def get_zabbixapp(domain):
    sql = "select `name` from api_dependent where domain='{0}' and type='app' or type='jumpApp' or type='mainApp';".format(domain)
    data = get_mysql_result(sql=sql)
    '''((u'app.ymatou.com',), (u'im.app.ymatou.com'))'''
    if data:
        r = []  
        for i in data:
            r.append(i[0])
        r = list(set(r))
        return r
    else:   
        return []

def get_data(domain, log_type, interval):
    table = log_type + 'log_' + interval
    sql = "select * from {0} where time>='{1}' and domain='{2}' and flag!=2 order by id desc LIMIT 1;".format(table, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-200)), domain)
    '''((192068L, u'2016-06-28 18:26:08', u'app.ymatou.com', 0.001, 5523L, 0L, 28L, 0L, 0L),)'''
    data = get_mysql_result(sql=sql)
    if data:
        if log_type == 'netscaler':
            return {'time': data[0][1],'domain':data[0][2], 'average_server_time':data[0][3], '2XX':data[0][4],'3XX':data[0][5], '4XX': data[0][6], '5XX':data[0][7]}
        else:
            return {'time': data[0][1],'domain':data[0][2], 'average_request_time':data[0][3], 'average_response_time':data[0][4], '2XX':data[0][5],'3XX':data[0][6], '4XX': data[0][7], '5XX':data[0][8]}
    else:
        return {}

def get_zabbixdata(domain):
    table = 'zabbixlog'
    mytime = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()-60))
    from_time = '{0}:00'.format(mytime)
    to_time = '{0}:59'.format(mytime)
    sql = "select * from %s where domain='%s' and time between '%s' and '%s';" %(table, domain, from_time, to_time)
    data = get_mysql_result(sql=sql)
    '''((2712385L, u'2016-06-29 11:26:03', u'user.app.ymatou.com', u'10.11.34.21', None, 0.0167),)'''
    if data:
        r = []  
        for i in data:
            r.append(dict(time=i[1], domain=i[2], host=i[3], connections=i[4], cpuload=i[5]))
        return r
    else:   
        return []

def get_analysis(domain):
    sql = "select data from `analysis` where time>='{0}' order by id desc limit 1;".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-180)))
    data = get_mysql_result(sql=sql)
    if data:
        return json.loads(data[0][0])
    else:
        return []

def get_event():
    mytime = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()-60))
    mytime2 = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()-120))
    from_time = '{0}:00'.format(mytime)
    to_time = '{0}:59'.format(mytime)
    from_time2 = '{0}:00'.format(mytime2)
    to_time2 = '{0}:59'.format(mytime2)
    sql = "select `time`,`type`,`parent`,`app`,`info`,`level` from event where time between '{0}' and '{1}';".format(from_time, to_time)
    data = get_mysql_result(sql=sql)
    if data:
        data = list(set(data))
    else:
        sql = "select `time`,`type`,`parent`,`app`,`info`,`level` from event where time between '{0}' and '{1}';".format(from_time2, to_time2)
        data = get_mysql_result(sql=sql)
        data = list(set(data))
    '''((u'2016-06-29 11:33:22', u'响应码', u'haproxy', u'api.productlist.ymatou.com', u'4XX占比上升3.901% 当前占比65.226% 最近10分钟占比61.325% 请求数1990', 2L),)'''
    if data:
        d = []
        for i in data:
            d.append({'time': i[0], 'type': i[1], 'parent': i[2], 'app': i[3], 'info': i[4], 'level': i[5]})
        return d
    else:   
        return []

def get_top():
    sql = "select `target` from kafkalog where time>='{0}';".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-60)))
    data = get_mysql_result(sql=sql)
    '''((u'http://app.ymatou.com/api/Home/GetOperationList',),)'''
    if data:
        dic = {}
        for i in data:
            if dic.get(i[0]):
                dic[i[0]] += 1
            else:
                dic[i[0]] = 1
        dict= sorted(dic.iteritems(), key=lambda d:d[1], reverse = True)
        d = []
        for i in dict[:10]:
            d.append({'domain':i[0], 'count': i[1]})
        return d
    else:
        return []
    
def get_kafka(domain, interval):
    forward_table = get_forward('app.ymatou.com')
    sql = "select `time`,`target`,`code`,`resp_time` from kafkalog where time>='{0}';".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()-interval)))
    data = get_mysql_result(sql=sql)
    '''((u'2016-06-29 13:46:06', u'http://app.ymatou.com/api/trading/scartprodnum', 200L, 256.0),)'''
    if data:
        log_time = data[-1][0]
        r = []
        d = {}
        for i in data:
            for k,v in forward_table.items():
                if k in i[1]:
                    if d.get(v):
                        d[v].append([i[2], i[3]])
                    else:
                        d[v] = [[i[2], i[3]]]
        for k,v in d.items():
            average_time = round(sum([i[1] for i in v])/len(v) / 1000, 3)
            tmp_d = {'2XX':0, '3XX':0, '4XX':0, '5XX':0}
            for i in v:
                if i[0] >=200 and i[0] < 300:
                    tmp_d['2XX'] += 1
                elif i[0] >=300 and i[0] < 400:
                    tmp_d['3XX'] += 1
                elif i[0] >=400 and i[0] < 500:
                    tmp_d['4XX'] += 1
                else:
                    tmp_d['5XX'] += 1
            r.append({'time':log_time, 'domain':k, 'average_resp_time':average_time, '2XX':tmp_d['2XX'],'3XX':tmp_d['3XX'],'4XX':tmp_d['4XX'],'5XX':tmp_d['5XX'] })
        for i in r:
            if i['domain'] == domain:
                return i
    else:
        return []

@api_view(['POST',])
@csrf_exempt
def zabbixdata(request):
    try:
        if request.method == 'POST':
            domain = request.POST.get('domain')
            if domain is None:
                return format_response(retcode=1, result='domain is mull')
            else:
                data = get_zabbixdata(domain)
                if data:
                    return format_response(retcode=0, result=data)
                else:
                    return format_response(retcode=1, result='no data')
        else:
            return format_response(retcode=1, result='not post request')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def data(request):
    try:
        if request.method == 'POST':
            domain = request.POST.get('domain')
            log_type = request.POST.get('log_type')
            interval = request.POST.get('interval')
            if log_type is None or interval is None or domain is None:
                return format_response(retcode=1, result='domain or log_type or interval is null')
            else:
                data = get_data(domain, log_type, interval)
                if data:
                    return format_response(retcode=0, result=data)
                else:
                    return format_response(retcode=1, result='no data')
        else:
            return format_response(retcode=1, result='not post request')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def analysis(request):
    try:
        if request.method == 'POST':
            domain = request.POST.get('domain')
            if domain is None:
                return format_response(retcode=1, result='domain is null')
            else:
                data = get_analysis(domain)
                if data:
                    return format_response(retcode=0, result=data)
                else:
                    return format_response(retcode=1, result='no data')
        else:
            return format_response(retcode=1, result='not post request')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def add_tree(request):
    try:
        if request.method == 'POST':
            data = json.loads(request.POST.get('data'))
            user_name = request.POST.get('user_name')
            if data is None or user_name is None:
                return format_response(retcode=1, result='data or user_name is null')
            else:
                t = tree.RelyTree()
                data = t.add(data=data, user_name=user_name)
                if data['retcode'] == 0:
                    return format_response(retcode=0, result=data['stdout'])
                else:
                    return format_response(retcode=1, result=data['stderr'])
        else:
            return format_response(retcode=1, result='not post request')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def get_tree(request):
    try:
        if request.method == 'POST':
            domain = request.POST.get('domain')
            if domain is None:
                return format_response(retcode=1, result='domain is null')
            else:
                t = tree.RelyTree()
                data = t.get(domain=domain)
                if data['retcode'] == 0:
                    return format_response(retcode=0, result=data['stdout'])
                else:
                    return format_response(retcode=1, result=data['stderr'])
        else:
            return format_response(retcode=1, result='not request post')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def delete_tree(request):
    try:
        if request.method == 'POST':
            id = request.POST.get('id')
            if id is None:
                return format_response(retcode=1, result='id is null')
            else:
                t = tree.RelyTree()
                data = t.delete(id=id)
                if data['retcode'] == 0:
                    return format_response(retcode=0, result=data['stdout'])
                else:
                    return format_response(retcode=1, result=data['stderr'])
        else:
            return format_response(retcode=1, result=data['not post request'])
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def zabbixapp(request):
    try:
        if request.method == 'POST':
            domain = request.POST.get('domain')
            if domain is None:
                return format_response(retcode=1, result='domain is null')
            else:
                data = get_zabbixapp(domain=domain)
                return format_response(retcode=0, result=data)
        else:
            return format_response(retcode=1, result='not post request')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def event(request):
    try:
        if request.method == 'POST':
            data = get_event()
            return format_response(retcode=0, result=data)
        else:
            return format_response(retcode=1, result='not post request')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def top(request):
    try:
        if request.method == 'POST':
            data = get_top()
            return format_response(retcode=0, result=data)
        else:
            return format_response(retcode=1, result='not post request')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)

@api_view(['POST',])
@csrf_exempt
def kafka(request):
    try:
        if request.method == 'POST':
            domain = request.POST.get('domain')
            interval = int(request.POST.get('interval'))
            if interval is None:
                return format_response(retcode=1, result='interval is null')
            else:
                data = get_kafka(domain, interval)
                return format_response(retcode=0, result=data)
        else:
            return format_response(retcode=1, result='not post request')
    except Exception as e:
        print e
        return format_response(retcode=2, result=e.message)
