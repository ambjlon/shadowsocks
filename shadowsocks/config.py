#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging

#Config
MYSQL_HOST = '45.78.57.84'
MYSQL_PORT = 56199
MYSQL_USER = 'ss'
MYSQL_PASS = 'lBe%tNKf'
MYSQL_DB = 'shadowsocks'

MANAGE_PASS = 'passwd'
#if you want manage in other server you should set this value to global ip
MANAGE_BIND_IP = '127.0.0.1'
#make sure this port is idle
MANAGE_PORT = 23333
#BIND IP
#if you want bind ipv4 and ipv6 '[::]'
#if you want bind all of ipv4 if '0.0.0.0'
#if you want bind all of if only '4.4.4.4'
SS_BIND_IP = '0.0.0.0'
SS_METHOD = 'rc4-md5'

#LOG CONFIG
LOG_ENABLE = True
LOG_LEVEL = logging.WARNING
LOG_FILE = '/home/ss/shadowsocks.log'

#本机ip
HOST_IP = ''
#表示为哪些用户提供服务 e.g. '0:2'
PAY_STATUS = '0:2'
