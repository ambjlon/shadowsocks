#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import cymysql
import time
import sys
import socket
import config
import json
import collections
import sets

class DbTransfer(object):

    instance = None

    def __init__(self):
        self.service4 = config.PAY_STATUS.split(':')
        self.last_get_transfer = {}
        self.traffic_logs = collections.defaultdict(long)
        self.active_time = {}
        #if there was many user, this dic is too large in memeory
        self.port2userid = {}
        # pull port2userid from db, and take it to memeory
        conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER,
                               passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute("SELECT id,port FROM user")
        rows = []
        for r in cur.fetchall():
            rows.append(list(r))
        cur.close()
        conn.close()
        for row in rows:
            self.port2userid[unicode(row[1])] = row[0]
        # get local ip
        #localhost = socket.getfqdn(socket.gethostname())
        #self.ip = socket.gethostbyname(localhost)
        #some machine, we have to specify selfip .
	self.ip = config.HOST_IP
	# ip2nodeid
        self.ip2nodeid = collections.defaultdict(int)
        #pull nodeid2ip from db, and take it to memeory
        conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER,
                               passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute("SELECT id,server FROM ss_node")
        rows = []
        for r in cur.fetchall():
            rows.append(list(r))
        cur.close()
        conn.close()
        self.node_id = -1
        for row in rows:
            if row[1] == self.ip:
                self.node_id = row[0]
        if self.node_id < 0:
            logging.error('this machine is not added into ss-panel.')
            sys.exit(0)
    @staticmethod
    def get_instance():
        if DbTransfer.instance is None:
            DbTransfer.instance = DbTransfer()
        return DbTransfer.instance

    @staticmethod
    def send_command(cmd):
        data = ''
        try:
            cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            cli.settimeout(1)
            cli.sendto(cmd, ('%s' % (config.MANAGE_BIND_IP), config.MANAGE_PORT))
            data, addr = cli.recvfrom(1500)
            cli.close()
            # TODO: bad way solve timed out
            time.sleep(0.05)
        except:
            logging.warn('send_command response')
        return data

    @staticmethod
    def get_servers_transfer():
        dt_transfer = {}
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2)
        cli.sendto('transfer: {}', ('%s' % (config.MANAGE_BIND_IP), config.MANAGE_PORT))
        bflag = False
        # in manage.py ports's transfer is sended in batched because 'STAT_SEND_LIMIT', and e reprsent end @chenjianglong
        while True:
            data, addr = cli.recvfrom(1500)
            if data == 'e':
                break
            data = json.loads(data)
            print data
            dt_transfer.update(data)
        cli.close()
        return dt_transfer

    @staticmethod
    def update_ports_active_time():
        active_time = {}
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2)
        cli.sendto('latest: {}', ('%s' % (config.MANAGE_BIND_IP), config.MANAGE_PORT))
        bflag = False
        while True:
            data, addr = cli.recvfrom(1500)
            if data == 'e':
                break
            data = json.loads(data)
            print data
            active_time.update(data)
        cli.close()
        return active_time
    def push_user_trafficlog(self):
        self.active_time.update(self.update_ports_active_time())
        keys = self.active_time.keys()
        now = int(time.time())
        for k in keys:
            #if a user port was not used in 45 mins then this port's traffic is loged. log with traffic<60 is ignored.
            if now - self.active_time[k] > 2700:
                # and self.traffic_logs[k] > 256: using iptables in 45.78.57.84 to forbidden port scanning is useful.  need to improve iptables usage. 
                # a user has only one port. 
                user_id = self.port2userid[k]
                # u and d is equal; what the fuck traffic is?
                query_sql = "INSERT INTO user_traffic_log (user_id,u,d,node_id,rate,traffic,log_time) VALUES(%s,%s,%s,%s,%s,'%s',now())" % (user_id,self.traffic_logs[k],self.traffic_logs[k],self.node_id,1.0,'')
                conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER, passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
                cur = conn.cursor()
                cur.execute(query_sql)
                cur.close()
                conn.commit()
                conn.close()
                self.traffic_logs[k] = 0
                del self.active_time[k]

    def push_user_onlinelog(self):
        keys = self.active_time.keys()
        now = int(time.time())
        online_user = 0
        online_user_set = set();
        for k in keys:
            if now - self.active_time[k] < 20:
                online_user += 1
                online_user_set.add(k)
                
        query_sql = "INSERT INTO ss_node_online_log_noid (node_id,server,online_user,log_time) VALUES(%s,'%s',%s,now()) ON DUPLICATE KEY UPDATE online_user=%s,log_time=now()" % (self.node_id,self.ip,online_user,online_user)
	conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER, passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute(query_sql)
        cur.close()
        conn.commit()
        conn.close()
        # push to ss_user_node_online_log_noid
        query_sql = 'INSERT INTO ss_user_node_online_log_noid (port,node_id,server,online_status,log_time) VALUES '
        keys = self.port2userid.keys()
        for key in keys:
            if key not in online_user_set:
                query_sql += "(%s,%s,'%s',%s,now())," % (key,self.node_id,self.ip,0)
            else:
                query_sql += "(%s,%s,'%s',%s,now())," % (key,self.node_id,self.ip,1)
        query_sql = query_sql[:-1]
        query_sql += ' ON DUPLICATE KEY UPDATE online_status=values(online_status),log_time=now()'
        online_user_set.clear()
        conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER, passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute(query_sql)
        cur.close()
        conn.commit()
        conn.close()
        
    # this function is dropped.
    def push_trafficlog_onlinelog(self):
        self.active_time.update(self.update_ports_active_time())
        keys = self.active_time.keys()
        now = int(time.time())
        online_user = 0
        online_user_set = set();
        # I am sure that active_time and statics are written at the same time always
        for k in keys:
            if now - self.active_time[k] > 1800:
                # a user has only one port. 
                user_id = self.port2userid[k]
                # u and d is equal; what the fuck traffic is?
                query_sql = "INSERT INTO user_traffic_log (user_id,u,d,node_id,rate,traffic,log_time) VALUES(%s,%s,%s,%s,%s,'%s',%s)" % (user_id,self.traffic_logs[k],self.traffic_logs[k],self.node_id,1.0,'',now)
                conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER, passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
                cur = conn.cursor()
                cur.execute(query_sql)
                cur.close()
                conn.commit()
                conn.close()
                self.traffic_logs[k] = 0
                del self.active_time[k]
            else:
                online_user += 1
                online_user_set.add(k)

        # push to ss_node_online_log_noid
        query_sql = "INSERT INTO ss_node_online_log_noid (node_id,server,online_user,log_time) VALUES(%s,'%s',%s,now()) ON DUPLICATE KEY UPDATE online_user=%s,log_time=now()" % (self.node_id,self.ip,online_user,online_user)
	conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER, passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute(query_sql)
        cur.close()
        conn.commit()
        conn.close()
        # push to ss_user_node_online_log_noid
        query_sql = 'INSERT INTO ss_user_node_online_log_noid (port,node_id,server,online_status,log_time) VALUES '
        keys = self.port2userid.keys()
        for key in keys:
            if key not in online_user_set:
                query_sql += "(%s,%s,'%s',%s,now())," % (key,self.node_id,self.ip,0)
            else:
                query_sql += "(%s,%s,'%s',%s,now())," % (key,self.node_id,self.ip,1)
        query_sql = query_sql[:-1]
        query_sql += ' ON DUPLICATE KEY UPDATE online_status=values(online_status),log_time=now()'
        online_user_set.clear()
        conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER, passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute(query_sql)
        cur.close()
        conn.commit()
        conn.close()
        
    # why this function is not static. Maybe self is argument
    def push_db_all_user(self):
        dt_transfer = self.get_servers_transfer()
        query_head = 'UPDATE user'
        query_sub_when = ''
        query_sub_when2 = ''
        query_sub_in = None
        last_time = time.time()
        for id in dt_transfer.keys():
            query_sub_when += ' WHEN %s THEN u+%s' % (id, 0) # all in d
            query_sub_when2 += ' WHEN %s THEN d+%s' % (id, dt_transfer[id])
            if query_sub_in is not None:
                query_sub_in += ',%s' % id
            else:
                query_sub_in = '%s' % id
        if query_sub_when == '':
            return
        query_sql = query_head + ' SET u = CASE port' + query_sub_when + \
                    ' END, d = CASE port' + query_sub_when2 + \
                    ' END, t = ' + str(int(last_time)) + \
                    ' WHERE port IN (%s)' % query_sub_in
        # print query_sql
        conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER,
                               passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute(query_sql)
        cur.close()
        conn.commit()
        conn.close()
        # update traffic_logs @chenjianlong
        total_transfer = 0
        for k, v in dt_transfer.items():
            self.traffic_logs[k] += v
            total_transfer += v

        query_sql = "INSERT INTO node_load (node_id,server,`load`,log_time) VALUES (%s,'%s',%s,now()) ON DUPLICATE KEY UPDATE `load`=`load`+%s,log_time=now()" % (self.node_id, self.ip, total_transfer, total_transfer)
        conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER,
                               passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute(query_sql)
        cur.close()
        conn.commit()
        conn.close()
        

    @staticmethod
    def pull_db_all_user():
        conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER,
                               passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute("SELECT port, u, d, transfer_enable, passwd, switch, enable, id, pay_status FROM user")
        rows = []
        for r in cur.fetchall():
            rows.append(list(r))
        cur.close()
        conn.close()
        return rows

    @staticmethod
    def del_server_out_of_bound_safe(rows):
        service4 = DbTransfer.get_instance().service4
        for row in rows:
            server = json.loads(DbTransfer.get_instance().send_command('stat: {"server_port":%s}' % row[0]))
            if server['stat'] != 'ko':
                if row[5] == 0 or row[6] == 0:
                    #stop disable or switch off user
                    logging.info('db stop server at port [%s] reason: disable' % (row[0]))
                    DbTransfer.send_command('remove: {"server_port":%s}' % row[0])
                    #del DbTransfer.get_instance().port2userid[unicode(row[0])]
                elif row[1] + row[2] >= row[3]:
                    #stop out bandwidth user
                    logging.info('db stop server at port [%s] reason: out bandwidth' % (row[0]))
                    DbTransfer.send_command('remove: {"server_port":%s}' % row[0])
                    #del DbTransfer.get_instance().port2userid[unicode(row[0])]
                if server['password'] != row[4]:
                    #password changed
                    logging.info('db stop server at port [%s] reason: password changed' % (row[0]))
                    DbTransfer.send_command('remove: {"server_port":%s}' % row[0])
                    #del DbTransfer.get_instance().port2userid[unicode(row[0])]
                if row[8] not in service4:
                    #password changed
                    logging.info('db stop server at port [%s] reason: pay_status changed' % (row[0]))
                    DbTransfer.send_command('remove: {"server_port":%s}' % row[0])
                    #del DbTransfer.get_instance().port2userid[unicode(row[0])]
            else:
                if row[5] == 1 and row[6] == 1 and row[1] + row[2] < row[3] and row[8] in service4:
                    logging.info('db start server at port [%s] pass [%s]' % (row[0], row[4]))
                    DbTransfer.send_command('add: {"server_port": %s, "password":"%s"}'% (row[0], row[4]))
                    DbTransfer.get_instance().port2userid[unicode(row[0])] = row[7]
                    print('add: {"server_port": %s, "password":"%s"}'% (row[0], row[4]))

    @staticmethod
    def thread_db():
        import socket
        import time
        timeout = 30
        socket.setdefaulttimeout(timeout)
        while True:
            logging.warn('db loop')
            try:
                DbTransfer.get_instance().push_db_all_user()
                rows = DbTransfer.get_instance().pull_db_all_user()
                DbTransfer.get_instance().push_user_trafficlog()
                DbTransfer.get_instance().push_user_onlinelog()
                DbTransfer.del_server_out_of_bound_safe(rows)
            except Exception as e:
                import traceback
                traceback.print_exc()
                logging.warn('db thread except:%s' % e)
            finally:
                time.sleep(15)


#SQLData.pull_db_all_user()
#print DbTransfer.send_command("")
