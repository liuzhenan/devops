import pymysql
import srv
import sys


class connect(object):
    zksql='select t_tag.name,server.master_ip,t_zookeeper_info.port,`status`.name \
            from t_zookeeper_info left join t_tag on t_zookeeper_info.tag_id=t_tag.id \
            left join t_server_tag on t_tag.id=t_server_tag.tag_id \
            left join server on t_server_tag.server_id=server.id \
            left join `status` on server.status_id=`status`.id \
            where server.status_id=%s'

    def __init__(self,env):
        self.srv=srv.New(servers=['192.168.33.13'],service="mysql-troy",tags=["prod","master"])
        addr_port=(self.srv.get()).split(":")#addr_port[0]   addr_port[0]
        self.connection = pymysql.connect(host=addr_port[0], port=int(addr_port[1]), user='root', passwd='rootroot', db='troy',charset="utf8")
        self.cursor = self.connection.cursor(cursor=pymysql.cursors.DictCursor)
        self.env=env
        self.x=[]

    def cursor(self):
        return self.cursor

    def closeConnnection(self):
        """
        关闭当前连接
        """
        self.connection.close()

    def zk(self):
        self.cursor.execute(self.zksql,self.env)
        list=self.cursor.fetchall()
        for dict in list:
            ip=dict['master_ip']
            port=dict['port']
            url=':'.join(map(str,(ip,port)))
            self.x.append(url)
        return ','.join(self.x)









