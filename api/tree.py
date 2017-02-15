# -*- coding:utf-8 -*-
from models import Dependent as DependentModel
from collections import defaultdict

def get_result(retcode, result):
    if retcode == 0:
        return dict(retcode=retcode, stderr=None, stdout=result)
    else:
        return dict(retcode=retcode, stderr=result, stdout=None)

def try_except(func):
    def wrapped(*args, **kwargs):
        result = None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            result = dict(retcode=1, stderr=str(e.message), stdout=None)
        finally:
            return result
    return wrapped

class RelyTree(object):
    def __init__(self):
        self.model = DependentModel

    @try_except
    def add(self, data, user_name):
        '''
        往数据库中增加tree结构
        :param data: dict类型
        :param user_name:创建者
        :return:
        '''
        root = self.insert(data, user_name)
        self.recurse_add(data, user_name, root)
        return dict(retcode=0, stderr=None, stdout='add complete')

    @try_except
    def get(self, domain):
        '''
        获取tree结构
        :param domain: app名称
        :return:tree
        '''
        query_list = self.model.objects.filter(domain=domain).order_by('id')
        temp = self.query_group(query_list)
        re = self.make_tree(temp["root"], temp)[0]
        return dict(retcode=0, stderr=None, stdout=re)

    @try_except
    def remove(self, id):
        '''
        根据ID删除数据库对象
        :param id: 删除ID
        :return:
        '''
        d = self.model.objects.get(id=id)
        self.delete(d)
        return dict(retcode=0, stderr=None, stdout='delete complete')

    def delete(self, item):
        '''
        递归删除对象
        :param item:父id对象
        :return:
        '''
        q = self.model.objects.filter(pid=item)
        if q:
            for i in q:
                self.delete(i)
        item.delete()

    @try_except
    def put(self, data):
        pass

    def insert(self, data, user_name, pid=None):
        '''
        数据库中增加对象
        :param data: 插入对象数据
        :param user_name: 创建者
        :param pid: 父ID对象
        :return:
        '''
        return self.model.objects.create(domain=data["domain"],
                                         name=data["name"],
                                         type=data["type"],
                                         creator=user_name,
                                         last_modified_by=user_name,
                                         pid=pid)

    def update(self, data):
        pass

    def recurse_add(self, data, user_name, pid):
        '''
        递归增加
        :param data:
        :param pid:
        :return:
        '''
        if data.get("children", None):
            for i in data["children"]:
                d = self.insert(i, user_name, pid)
                self.recurse_add(i, user_name, d)

    def query_group(self, query_list):
        '''
        根据查询结果进行分组
        :param query_list: 查询结果数据集
        :return:
        '''
        re = defaultdict(list)
        for i in query_list:
            if i.pid:
                re[i.pid.id].append(i)
            else:
                re["root"].append(i)
        return re

    def make_tree(self, items, temp):
        '''
        组建tree结构
        :param items: 节点数据
        :param temp: 分组数据
        :return:
        '''
        return [{"id": i.id, "domain": i.domain, "name": i.name, "type": i.type,
                 "children": self.make_tree(temp[i.id], temp)} for i in items]
