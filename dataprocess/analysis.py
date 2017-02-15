# -*- coding: utf-8 -*-
import json
import requests
import ConfigParser
from store import event_store
from collections import Counter

cf = ConfigParser.ConfigParser()
cf.read('/opt/app/fulllink/dataprocess/config.ini')
config = {}
config['fulllink_data_url'] = cf.get('fulllink', 'data_url') 
config['fulllink_zabbixdata_url'] = cf.get('fulllink', 'zabbixdata_url') 
config['fulllink_kafka_url'] = cf.get('fulllink', 'kafka_url') 

class LogBase:
    def get_threshold(self):
        cf = ConfigParser.ConfigParser()
        cf.read('/opt/app/fulllink/dataprocess/threshold.ini')
        threshold = {}
        threshold['average_server_time'] = dict(warn=float(cf.get('aging', 'warn')), error=float(cf.get('aging', 'error')), absolute_warn=float(cf.get('aging', 'absolute_warn')), absolute_error=float(cf.get('aging', 'absolute_error')))
        threshold['average_response_time'] = dict(warn=float(cf.get('aging', 'warn')), error=float(cf.get('aging', 'error')), absolute_warn=float(cf.get('aging', 'absolute_warn')), absolute_error=float(cf.get('aging', 'absolute_error')))
        threshold['average_resp_time'] = dict(warn=float(cf.get('aging', 'warn')), error=float(cf.get('aging', 'error')), absolute_warn=float(cf.get('aging', 'absolute_warn')), absolute_error=float(cf.get('aging', 'absolute_error')))
        threshold['2XX'] = dict(warn=float(cf.get('2XX', 'warn')), error=float(cf.get('2XX', 'error')), absolute_warn=float(cf.get('2XX', 'absolute_warn')), absolute_error=float(cf.get('2XX', 'absolute_error')))
        threshold['3XX'] = dict(warn=float(cf.get('3XX', 'warn')), error=float(cf.get('3XX', 'error')), absolute_warn=float(cf.get('3XX', 'absolute_warn')), absolute_error=float(cf.get('3XX', 'absolute_error')))
        threshold['4XX'] = dict(warn=float(cf.get('4XX', 'warn')), error=float(cf.get('4XX', 'error')), absolute_warn=float(cf.get('4XX', 'absolute_warn')), absolute_error=float(cf.get('4XX', 'absolute_error')))
        threshold['5XX'] = dict(warn=float(cf.get('5XX', 'warn')), error=float(cf.get('5XX', 'error')), absolute_warn=float(cf.get('5XX', 'absolute_warn')), absolute_error=float(cf.get('5XX', 'absolute_error')))
        threshold['kafka_average_resp_time'] = dict(warn=float(cf.get('kafka_aging', 'warn')), error=float(cf.get('kafka_aging', 'error')))
        threshold['kafka_2XX'] = dict(warn=float(cf.get('kafka_2XX', 'warn')), error=float(cf.get('kafka_2XX', 'error')))
        threshold['kafka_3XX'] = dict(warn=float(cf.get('kafka_3XX', 'warn')), error=float(cf.get('kafka_3XX', 'error')))
        threshold['kafka_4XX'] = dict(warn=float(cf.get('kafka_4XX', 'warn')), error=float(cf.get('kafka_4XX', 'error')))
        threshold['kafka_5XX'] = dict(warn=float(cf.get('kafka_5XX', 'warn')), error=float(cf.get('kafka_5XX', 'error')))
        threshold['zabbix-connections'] = dict(warn=int(cf.get('zabbix-connections', 'warn')), error=int(cf.get('zabbix-connections', 'error')))
        threshold['zabbix-cpuload'] = dict(warn=float(cf.get('zabbix-cpuload', 'warn')), error=float(cf.get('zabbix-cpuload', 'error')))
        return threshold

    def get_data(self, domain, log_type, interval):
        try:
            data = {"domain": domain, "log_type": log_type, "interval": interval}
            url = config['fulllink_data_url']
            res = requests.post(url=url, data=data)
            return json.loads(res.text)["stdout"]
        except Exception as e:
            print e
            return None

    def get_kafka(self, domain, interval):
        try:
            data = {"domain":domain, "interval": interval}
            url = config['fulllink_kafka_url']
            res = requests.post(url=url, data=data)
            return json.loads(res.text)["stdout"]
        except Exception as e:
            print e
            return None

    def get_zabbixdata(self, domain):
        try:
            url = config['fulllink_zabbixdata_url']
            r = requests.post(url=url, data={'domain':domain})
            zabbixdata = json.loads(r.text)['stdout']
            return zabbixdata
        except Exception as e:
            print e
            return None

    def ten_seconds(self, domain, log_type):
        interval = '10s'
        return self.get_data(domain, log_type, interval)

    def twenty_seconds(self, domain, log_type):
        interval = '20s'
        return self.get_data(domain, log_type, interval)

    def one_minuter(self, domain, log_type):
        interval = '60s'
        return self.get_data(domain, log_type, interval)

    def ten_minuters(self, domain, log_type):
        interval = '600s'
        return self.get_data(domain, log_type, interval)

    def contact_data(self, domain, log_type):
        one = self.twenty_seconds(domain, log_type)
        ten = self.ten_minuters(domain, log_type)
        return one, ten

    def re_status(self, re):
        if 2 in re:
            return 2
        elif 1 in re:
            return 1
        else:
            return 0

    def rate_status(self, re):
        re_len = len(re)
        tag = Counter(re)

        tag_err = (round(tag[2], 3) / re_len) * 100
        tag_warn = (round(tag[1], 3) / re_len) * 100

        if tag_err >= 50:
            return 2
        elif tag_warn >= 20 or tag_err > 0:
            return 1
        else:
            return 0

    def compare(self, value, th, absolute_value=None, total=None, type=None):
        re = []
        if absolute_value == 'zabbix':
            if value >= th["warn"]:
                re.append(1)
            else:
                re.append(0)
        else:
            if type == 'aging':
                if (value >= th["error"] or absolute_value >= th["absolute_error"]) and absolute_value>0.3 and total > 10:
                    re.append(2)
                elif (value >= th["warn"] or absolute_value >= th["absolute_warn"]) and absolute_value>0.1 and total > 5:
                    re.append(1)
                else:
                    re.append(0)
            else:
                if (value >= th["error"] or absolute_value >= th["absolute_error"]) and absolute_value>5 and total > 10:
                    re.append(2)
                elif (value >= th["warn"] or absolute_value >= th["absolute_warn"]) and absolute_value>2 and total > 5:
                    re.append(1)
                else:
                    re.append(0)
        return re

    def event(self, log_time, type, data):
        if type == 'aging':
            parent = data[0]
            app = data[1]
            total = data[4]
            info = '耗时上升{0}s 当前耗时{1}s 最近10分钟耗时{2}s 请求数{3}'.format(data[2]-data[3], data[2], data[3], total)
            event_store(log_time=log_time, type='耗时', parent=parent, app=app, info=info, level=data[-1])
        elif type == 'code':
            parent = data[0]
            app = data[1]
            total = data[6]
            info = '{0}占比上升{1}% 当前占比{2}% 最近10分钟占比{3}% 请求数{4}'.format(data[2], data[4]-data[5], data[4], data[5], total)
            event_store(log_time=log_time, type='响应码', parent=parent, app=app, info=info, level=data[-1])
        elif type == 'zabbix-cpuload':
            parent = data[0]
            app = data[1]
            info = 'Host:{0}  CPU负载{1}%'.format(data[2], data[3])
            event_store(log_time=log_time, type='Zabbix', parent=parent, app=app, info=info, level=data[-1])
        elif type == 'zabbix-connections':
            parent = data[0]
            app = data[1]
            info = 'Host:{0}  IIS连接数{1}'.format(data[2], data[3])
            event_store(log_time=log_time, type='Zabbix', parent=parent, app=app, info=info, level=data[-1])
        if type == 'kafka-aging':
            parent = data[0]
            app = data[1]
            total = data[4]
            info = '耗时上升{0}s 当前耗时{1}s 最近10分钟耗时{2}s 请求数{3}'.format(data[2]-data[3], data[2], data[3], total)
            event_store(log_time=log_time, type='kafka耗时', parent=parent, app=app, info=info, level=data[-1])
        elif type == 'kafka-code':
            parent = data[0]
            app = data[1]
            total = data[6]
            info = '{0}占比上升{1}% 当前占比{2}% 最近10分钟占比{3}% 请求数{4}'.format(data[2], data[3], data[4], data[5], total)
            event_store(log_time=log_time, type='kafka响应码', parent=parent, app=app, info=info, level=data[-1])

    def run(self, data, app_name, parent_type=None):
        o_type = data["type"]
        if o_type == 'netscaler':
            data = Netscaler().solver(data, app_name, parent_type)
        elif o_type == "extranet_nginx":
            data = NginxOuter().solver(data, app_name, parent_type)
        elif o_type == "mainApp":
            data = MainApp().solver(data, app_name, parent_type)
        elif o_type == "jumpApp":
            data = JumpApp().solver(data, app_name, parent_type)
        elif o_type == "haproxy":
            data = Haproxy().solver(data, app_name, parent_type)
        elif o_type == "intranet_nginx":
            data = NginxInner().solver(data, app_name, parent_type)
        elif o_type == "app":
            data = App().solver(data, app_name, parent_type)
        return data

class Netscaler(LogBase):
    type = 'netscaler'

    def solver(self, data, app_name, parent_type=None):
        re = []
        if data.get("children"):
            for i in data["children"]:
                self.run(i, app_name)
                re.append(i["status"])

        one, ten = self.contact_data(app_name, self.type)
        if one['average_server_time'] == 0.0:
            one['average_server_time'] = 0.001
        if one and one.get("average_server_time"):
            data["diff"] = one["average_server_time"]
            re.append(self.judgement(one, ten))
        else:
            data["diff"] = '!'
            re.append(0)

        data["status"] = self.rate_status(re)

        return data

    def judgement(self, one, ten):
        total = one.get('2XX', 0) + one.get('3XX', 0) + one.get('4XX', 0) + one.get('5XX', 0)
        th = self.get_threshold()
        if one["average_server_time"] and ten["average_server_time"]:
            value = round(one["average_server_time"] / ten["average_server_time"], 2)
            absolute_value = one["average_server_time"] 
            re = self.compare(value, th["average_server_time"], absolute_value, total, 'aging')
            if re != [0]:
                log_time = one['time']
                data = [type, one['domain'], one["average_server_time"], ten["average_server_time"], re[0]]
                self.event(log_time=log_time, type='aging', data=data)
        else:
            re = [0]
        return self.re_status(re)

class NginxOuter(LogBase):
    type = 'extranet_nginx'

    def solver(self, data, app_name, parent_type=None):
        re = []
        if data.get("children"):
            for i in data["children"]:
                self.run(i, app_name, data["type"])
                re.append(i["status"])

        one, ten = self.contact_data(app_name, self.type)
        if one and one.get("average_response_time"):
            data["diff"] = one["average_response_time"]
            re.append(self.judgement(one, ten, parent_type))
        else:
            data["diff"] = '!'
            re.append(0)

        data["status"] = self.rate_status(re)
        return data

    def judgement(self, one, ten, parent_type):
        total = one.get('2XX', 0) + one.get('3XX', 0) + one.get('4XX', 0) + one.get('5XX', 0)
        th = self.get_threshold()
        if one["average_response_time"] and ten["average_response_time"]:
            value = round(one["average_response_time"] / ten["average_response_time"], 2)
            absolute_value = one["average_response_time"]
            re = self.compare(value, th["average_response_time"], absolute_value, total, 'aging')
            if re != [0]:
                log_time = one['time']
                data = [type, one['domain'], one["average_response_time"], ten["average_response_time"], re[0]]
                self.event(log_time=log_time, type='aging', data=data)
        else:
            re = [0]
        return self.re_status(re)

class MainApp(LogBase):
    def solver(self, data, app_name, parent_type=None):
        data["response_time_total"] = 0
        data["app_length"] = 0
        re = []
        
        zabbixdata = self.get_zabbixdata(domain=data['name'])

        if data.get("children"):
            for i in data["children"]:
                self.run(i, app_name, parent_type)
                re.append(i["status"])
                data["response_time_total"] += i["response_time_total"]
                data["app_length"] += i["app_length"]
            data["response_time_total"] = round(data["response_time_total"], 3)

        one, ten = self.contact_data(data["name"], parent_type)
        if one and one.get("average_response_time"):
            data["diff"] = one["average_response_time"]
            re.append(self.judgement(one, ten, parent_type, zabbixdata=zabbixdata))
        else:
            data["diff"] = '!'
            re.append(0)

        data["status"] = self.rate_status(re)
        return data

    def judgement(self, one, ten, parent_type, kafka_one=None, kafka_ten=None, zabbixdata=None):
        th = self.get_threshold()
        re = []
        total = one.get('2XX', 0) + one.get('3XX', 0) + one.get('4XX', 0) + one.get('5XX', 0)
        ten_total = ten.get('2XX', 0) + ten.get('3XX', 0) + ten.get('4XX', 0) + ten.get('5XX', 0)
        xx_key = ["3XX", "4XX", "5XX"]  
        for i in one:
            if i == 'average_response_time':
                thi = th[i]
                if one[i] and ten[i]:
                    value = round(one[i] / ten[i], 2)
                    absolute_value = one[i] 
                    tmp_re = self.compare(value, thi, absolute_value, total, 'aging')
                    re += tmp_re
                    if tmp_re != [0]:
                        log_time = one['time']
                        data = [parent_type, one['domain'], one["average_response_time"], ten["average_response_time"], total, tmp_re[0]]
                        self.event(type='aging', log_time=log_time, data=data)
                else:
                    re.append(0)
            elif i in xx_key:
                thi = th[i]
                if total and ten_total:
                    rate_one = round(((round(one[i], 3) / total) * 100), 3)
                    rate_ten = round(((round(ten[i], 3) / ten_total) * 100), 3)
                    if rate_one and rate_ten:
                        value = round(rate_one / rate_ten, 2)
                        absolute_value = rate_one
                        tmp_re = self.compare(value, thi, absolute_value, total)
                        re += tmp_re
                        if tmp_re != [0]:
                            log_time = one['time']
                            data = [parent_type, one['domain'], i, value, rate_one, rate_ten, total, tmp_re[0]]
                            self.event(type='code', log_time=log_time, data=data)
                    else:
                        re.append(0)
                else:
                    re.append(0)
        if zabbixdata:
            for i in zabbixdata:
                thi = th['zabbix-connections']
                value = i['connections']
                tmp_re = self.compare(value, thi, 'zabbix')
                re += tmp_re
                if tmp_re != [0]:
                    log_time = i['time']
                    data = [parent_type, one['domain'], i['host'], i['connections'], tmp_re[0]]
                    self.event(type='zabbix-connections', log_time=log_time, data=data)
                thi = th['zabbix-cpuload']
                value = i['cpuload']
                tmp_re = self.compare(value, thi, 'zabbix')
                re += tmp_re
                if tmp_re != [0]:
                    log_time = i['time']
                    data = [parent_type, one['domain'], i['host'], round(i.get('cpuload', 0), 3), tmp_re[0]]
                    self.event(type='zabbix-cpuload', log_time=log_time, data=data)
                else:
                    re.append(0)
        if kafka_one:
            th = self.get_threshold()
            re = []
            total = kafka_one.get('2XX', 0) + kafka_one.get('3XX', 0) + kafka_one.get('4XX', 0) + kafka_one.get('5XX', 0)
            kafka_ten_total = kafka_ten.get('2XX', 0) + kafka_ten.get('3XX', 0) + kafka_ten.get('4XX', 0) + kafka_ten.get('5XX', 0)
            xx_key = ["3XX", "4XX", "5XX"]  
            for i in kafka_one:
                if i == 'average_resp_time':
                    thi = th['kafka_'+i]
                    if kafka_one[i] and kafka_ten[i]:
                        value = round(kafka_one[i] / kafka_ten[i], 2)
                        absolute_value =  kafka_one[i]
                        tmp_re = self.compare(value, thi, absolute_value, total, 'aging')
                        re += tmp_re
                        if tmp_re != [0]:
                            log_time = kafka_one['time']
                            data = [parent_type, kafka_one['domain'], kafka_one["average_resp_time"], kafka_ten["average_resp_time"], total, tmp_re[0]]
                            self.event(type='kafka-aging', log_time=log_time, data=data)
                    else:
                        re.append(0)
                elif i in xx_key:
                    thi = th['kafka_'+i]
                    if total and kafka_ten_total:
                        rate_kafka_one = round(((round(kafka_one[i], 3) / total) * 100), 3)
                        rate_kafka_ten = round(((round(kafka_ten[i], 3) / kafka_ten_total) * 100), 3)
                        if rate_kafka_one and rate_kafka_ten:
                            value = round(rate_kafka_one / rate_kafka_ten, 2)
                            absolute_value = rate_kafka_one
                            tmp_re = self.compare(value, thi, absolute_value, total)
                            re += tmp_re
                            if tmp_re != [0]:
                                log_time = kafka_one['time']
                                data = [parent_type, kafka_one['domain'], i, value, rate_kafka_one, rate_kafka_ten, total, tmp_re[0]]
                                self.event(type='kafka-code', log_time=log_time, data=data)
                        else:
                            re.append(0)
                    else:
                        re.append(0)
        return self.re_status(re)

class JumpApp(MainApp):
    def solver(self, data, app_name, parent_type=None):
        data["response_time_total"] = 0
        data["app_length"] = 0
        re = []
        
        zabbixdata = self.get_zabbixdata(domain=data['name'])

        if data.get("children"):
            for i in data["children"]:
                self.run(i, app_name, parent_type)
                re.append(i["status"])
                data["response_time_total"] += i["response_time_total"]
                data["app_length"] += i["app_length"]
            data["response_time_total"] = round(data["response_time_total"], 3)

        one, ten = self.contact_data(data["name"], parent_type)
        kafka_one = self.get_kafka(data['name'], 90)
        kafka_ten = self.get_kafka(data['name'], 600)
        if one and one.get("average_response_time"):
            data["diff"] = one["average_response_time"]
            re.append(self.judgement(one, ten, parent_type, kafka_one, kafka_ten, zabbixdata))
        else:
            data["diff"] = '!'
            re.append(0)

        data["status"] = self.rate_status(re)
        return data

class Haproxy(LogBase):
    type = 'haproxy'

    def solver(self, data, app_name, parent_type=None):
        data["response_time_total"] = 0
        data["app_length"] = 0
        re = []
        if data.get("children"):
            for i in data["children"]:
                self.run(i, app_name, self.type)
                re.append(i["status"])
                data["response_time_total"] += i["response_time_total"]
                data["app_length"] += i["app_length"]
            data["response_time_total"] = round(data["response_time_total"], 3)
        if data["app_length"]:
            data["diff"] = round(data["response_time_total"] / data["app_length"], 3)
        else:
            data["diff"] = '!'

        data["status"] = self.rate_status(re)

        return data

class NginxInner(Haproxy):
    type = 'intranet_nginx'

class App(MainApp):
    def solver(self, data, app_name, parent_type=None):
        data["response_time_total"] = 0
        data["status"] = 0
        data["app_length"] = 0
        re = []
        
        zabbixdata = self.get_zabbixdata(domain=data['name'])

        if data.get("children"):
            for i in data["children"]:
                self.run(i, app_name, parent_type)
                re.append(i["status"])
                data["app_length"] += i["app_length"]
                data["response_time_total"] += i["response_time_total"]
            data["response_time_total"] = round(data["response_time_total"], 3)

        one, ten = self.contact_data(data["name"], parent_type)
        if one and one.get("average_response_time"):
            data["app_length"] += 1
            data["response_time_total"] += one["average_response_time"]
            data["diff"] = one["average_response_time"]
            re.append(self.judgement(one, ten, parent_type, zabbixdata=zabbixdata))
        else:
            data["diff"] = '!'
            re.append(0)

        data["status"] = self.rate_status(re)

        return data

class Solver(object):
    def package(self, data, app_name):
        re = LogBase().run(data, app_name)
        return re

if __name__ == '__main__':
    a =  MainApp()
    #print a.get_data('vc0.xlobo.com', 'haproxy', '20s')
    print a.get_kafka('im.app.ymatou.com', 6000)
