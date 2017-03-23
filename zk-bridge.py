#!/usr/bin/env python
#-*-coding:utf-8
from kazoo.client import KazooClient
from kazoo.security import *
from kazoo.exceptions import *
from flask import Flask
from flask_jsonrpc import JSONRPC,site
import sys
import os,time
import argparse
import logging
import configparser
from conn_db import *




logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s,PID:%(process)d',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/var/log/zk.log',
                filemode='a')


app = Flask(__name__)
jsonrpc = JSONRPC(app, '/api')






def CreateNode(path, value=b'None', acl=None,
               ephemeral=False,sequence=False,
               makepath=False,Recursively=False):

    if Recursively == True and value:
        raise  ValueError('Only Options Value or Recursively')
    else:
        zk.start()
        if Recursively == True and not value:
            zk.ensure_path(path, acl)
        else:
            zk.create(path, value, acl, ephemeral, sequence, makepath)
        zk.stop()

def DeleteNode(path=None, version=-1, recursive=False):
    zk.start()
    zk.delete(path, version, recursive)
    zk.stop()

def GetNode(path, watch=None, include_data=None):
    """

    :rtype : object
    """

    zk.start()
    if include_data:
        value, Stat=zk.get_children(path, watch, include_data)
    else:
        value, Stat=zk.get(path, watch)
    zk.stop()
    value.sort()

def SetNode(path, value, version=-1):
    zk.start()
    zk.set(path,value,version)
    zk.stop()

list=[]   #需要初始化的zookeeper结果列表



def deco(func):
    def _deco(*args, **kwargs):
        env=kwargs['env']
        obj=connect(env)
        url=obj.zk()
        #print('url',url)
        zk = KazooClient(url)
        if kwargs.get('path'):
            prefix='/platform' 
            kwargs['path'] = prefix + kwargs['path']
        zk.start()
        ret = func(zk=zk,*args, **kwargs)
        zk.stop()
        return ret
    return _deco




@jsonrpc.method('load')
@deco
def load(filestream,env=None,zk=None):
    try:
        for line in filestream:
            line='/platform' + line
            list=line.split('=',maxsplit=1)


            if zk.exists(list[0]) != None:
                zk.set(list[0],list[1].encode())

            else:
                zk.ensure_path(list[0])
                zk.set(list[0],list[1].encode())
        return True
    except Exception as e:
        return e
        #print(e)
    

#




@jsonrpc.method('dump')
@deco
def dump(path,env=None,zk=None):
    #print('dumppath=',path)
    try:
        del list[:]
        subdump(path,zk)
        return list
    except Exception as e:
        return e




def subdump(path,zk):
    try:
        if zk.get_children(path):       #第一次判断必须以子节点为迭代条件
            nodes=zk.get_children(path)
            for node in nodes:
                new_path=path+'/'+node
                if len(zk.get_children(new_path)) >= 0:   #以路径是否有子节点为迭代条件
                #if value == None or len(value) == 0:   #以路径是否有值为迭代条件
                    subdump(new_path,zk)
        else:
            new_value,new_status=zk.get(path)
            if new_value == None:
                new_value=b""
            result=path+'='+new_value.decode()
            list.append(result[9:])
    except Exception as e:
        raise Exception(path,e)

@jsonrpc.method('exists')
@deco
def Exists(path, zk=None, watch=None):
    try:
        if zk.exists(path):
            return True
        else:
            return False
    except Exception as err:
        return


@jsonrpc.method('create')
@deco
def Create(path=None, value="", acl=None,sequence=False,Recursively=False,env=None,zk=None):
    if Recursively and value:
        raise ValueError("the recursively=True can't create when you put value='' args")
    elif Recursively:
        try:
            zk.ensure_path(path, acl)
            return True
        except Exception as err:
            return False
    else:
        try:
            if value =="":
                value=""
            if value is None:
                value=""
            if value is not None:
                value=str.encode(value)
            zk.create(path, value, acl, sequence)
            return True
        except Exception as err:
            return err

@jsonrpc.method('delete')
@deco
def Delete(path, version=-1, recursive=False,env=None,zk=None): 
    try:
        zk.delete(path, version, recursive)
        return True
    except Exception as err:
        return False




@jsonrpc.method('get')
@deco
def Get(path, watch=None, include_data=None,env=None,zk=None):
    '''zookeeper获取数据与获取子节点'''
    if include_data:
        include_data=True
        value, Stat=zk.get_children(path, watch, include_data)
        value.sort()
        return value
    else:
        value, Stat=zk.get(path, watch)
        if not value:
            return None
        elif isinstance(value,bytes) or len(value) >= 0:
            return value.decode(encoding="utf-8")
        else:
            value.sort()
            return value

@jsonrpc.method('set')
@deco
def Set(path, value="", version=-1,env=None,zk=None):
    if value==None:
        try:
            zk.set(path,value,version)
            return True
        except Exception as err:
            return err
    else:
        value=str.encode(value)
        try:
            zk.set(path,value,version)
            return True
        except Exception as err:
            return err

if __name__ == '__main__':
    if len(sys.argv[1:]) > 0:
        parser = argparse.ArgumentParser(description='CREATE,DELETE,SET,GET method operate zookeeper')
        parser.add_argument('--host', default='127.0.0.1', help='connect host, --host "IP"')
        parser.add_argument('--port', default='2181', help='connect host port, --port "port"')
        parser.add_argument('--action', help='choices method, --action "create"',choices=['create', 'delete', 'set', 'get'])
        parser.add_argument('--path', help='path of znode')
        parser.add_argument('--data',default='', help='znode data')
        parser.add_argument('-r', '--recursively',action='store_true',help='whether recursively create')
        parser.add_argument('-e', '--ephemeral',action='store_true',help='create ephemeral znode')
        parser.add_argument('-s', '--sequence',action='store_true',help='create sequence znode')
        parser.add_argument('--acl',help='define acl and you must provide a dictionary,for example:{"perms":3,"scheme":"ip","id":"192.168.0.0"} ACL scheme chiose (world,auth,digest,ip,super,sasl)')
        parser.add_argument('--timeout',help='zookeeper connect timeout',type=int,default=10.0)
        parser.add_argument('--read_only',action='store_false',help='read only')
        parser.add_argument('-v', '--version',default=-1,type=int,help='znode version')
        args = parser.parse_args()
        zk = KazooClient(hosts="%s:%s" % (args.host,args.port),
                     timeout=args.timeout, client_id=None, handler=None,
                     default_acl=None, auth_data=None, read_only=None,
                     randomize_hosts=True, connection_retry=None,
                     command_retry=None, logger=None)
        args.datas=args.data.encode(encoding="utf-8")
#
        if args.action == 'create':
            if isinstance(args.acl,str):
                acl=eval(args.acl)
                ACLS=[ACL(perms=acl['perms'], id=Id(scheme=acl['scheme'], id=acl['id']))]
            else:
                ACLS=None
            CreateNode(path=args.path, value=args.datas, acl=ACLS,
                   ephemeral=args.ephemeral,sequence=args.sequence,
                   makepath=False,Recursively=args.recursively)
        elif args.action == 'set':
            SetNode(path=args.path,value=args.datas, version=-1)
        elif args.action == 'get':
            GetNode(path=args.path, watch=None, include_data=None)
        elif args.action == 'delete':
            DeleteNode(path=args.path, version=args.version, recursive=args.recursively)
        else:
            parser.print_usage()
    else:
        app.run(host='0.0.0.0',port=8081, processes=10,debug=True,use_reloader=True)






