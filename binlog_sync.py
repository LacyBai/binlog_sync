#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import redis, pymysql
import optparse
import datetime
from get_yml import get_yml
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
from pymysqlreplication.event import (GtidEvent)
from pymysqlreplication.gtid import Gtid

if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False

## 建立redis链接池
def redis_pool(kwargs):
    try:
        pool = redis.ConnectionPool(**kwargs)
        r = redis.StrictRedis(connection_pool=pool)
        return r
    except Exception as e:
        raise e

## mysql链接
def con_mysql(kwargs):
    try:
        con = pymysql.connect(**kwargs)
        cursor = con.cursor()
        return con, cursor
    except Exception as e:
        raise e

## 获取当前gtid_executed
def get_gtid(kwargs):
    try:
        sql = "SELECT @@global.gtid_executed;"
        con, cursor = con_mysql(kwargs)
        cursor.execute(sql)
        return cursor.fetchone()[0]
    except Exception as e:
        raise e

## 生成gtid
def set_gtid(current_gtid, next_gtid):
    sid = next_gtid.split(':')[0]
    tr_id = next_gtid.split(':')[1]
    re_gtid = ''
    try:
        gtid = current_gtid.replace('\n', '')
        if gtid:
            for i in gtid.split(','):
                if i.find(sid) != 0:
                    re_gtid = re_gtid + ',' + i
            f_gtid = re_gtid + ',' + sid + ':1-' + tr_id
            return f_gtid.strip(',')
        else:
            return sid + ':1-' + tr_id
    except Exception as e:
        raise e

## 再目标mysql实例同步binlog
def sync_binlog(con, cursor, sql):
    try:
        cursor.execute(sql)
        con.commit()
    except Exception as e:
        raise e

def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k

def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return value.decode('utf-8')
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value

## 主逻辑函数，在源mysql实例订阅binglog事件
def main():
    stream = BinLogStreamReader(connection_settings=SMYSQL_SETTINGS,
                                server_id=server_id,
                                resume_stream=True,
                                auto_position=gtid_next,
                                only_schemas=only_schemas,
                                ignored_schemas=ignored_schemas,
                                only_tables=only_tables,
                                ignored_tables=ignored_tables,
                                only_events=[DeleteRowsEvent,WriteRowsEvent,UpdateRowsEvent, GtidEvent])

    con = pymysql.connect(**SMYSQL_SETTINGS)
    template = ''
    values = []
    cursor = con.cursor()
    redis_con = redis_pool(REDIS_SETTINGS)
    for binlogevent in stream:
        if isinstance(binlogevent, GtidEvent):
            pre_gtid_next = redis_con.hget('gtid_next', redis_chanel)
            redis_con.hset('pre_gtid_next', redis_chanel, pre_gtid_next)
            redis_con.hset('gtid_next', redis_chanel,  binlogevent.gtid)
        else:
            #schema = binlogevent.schema
            if target_schemas.has_key(binlogevent.schema):
                schema = target_schemas[binlogevent.schema]
            else:
                schema = binlogevent.schema
            table = binlogevent.table
            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(schema, table, ' AND '.join(map(compare_items, row['values'].items())))
                    values = map(fix_object, row['values'].values())
                elif isinstance(binlogevent, UpdateRowsEvent):
                    template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(schema,
                                table, ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]), ' AND '.join(map(compare_items, row['before_values'].items())))
                    values = map(fix_object, list(row['after_values'].values())+list(row['before_values'].values()))
                elif isinstance(binlogevent, WriteRowsEvent):
                    template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES({3});'.format(schema,
                                table, ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())), ', '.join(['%s'] * len(row['values'])))
                    values = map(fix_object, row['values'].values())
                sql = cursor.mogrify(template, values)
                #redis_con.publish(redis_chanel, sql)
                con, cursor = con_mysql(DMYSQL_SETTINGS)
                sync_binlog(con, cursor, sql)
    stream.close()


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', dest='config', type=str, help='The config file', default='')
    parser.add_option('-i', '--server_id', dest='server_id', type=int, help='The server-id', default=123456)
    parser.add_option('-g', '--gtid', dest='gtid', type=str, help='Start from where[pre_gtid_next|gtid_next]', default='')
    (options, args) = parser.parse_args()

    server_id = options.server_id
    cnf = options.config
    gtid = options.gtid

    get_yml = get_yml()
    REDIS_SETTINGS = get_yml.yml_dict(cnf)['REDIS_SETTINGS']
    SMYSQL_SETTINGS = get_yml.yml_dict(cnf)['SMYSQL_SETTINGS']
    DMYSQL_SETTINGS = get_yml.yml_dict(cnf)['DMYSQL_SETTINGS']
    redis_chanel = get_yml.yml_dict(cnf)['CHANEL']['redis_chanel']
    only_schemas = get_yml.yml_dict(cnf)['ONLY_CNF']['only_schemas']
    target_schemas = get_yml.yml_dict(cnf)['ONLY_CNF']['target_schemas']
    only_tables = get_yml.yml_dict(cnf)['ONLY_CNF']['only_tables']
    ignored_schemas = get_yml.yml_dict(cnf)['IGNORED_CNF']['ignored_schemas']
    ignored_tables = get_yml.yml_dict(cnf)['IGNORED_CNF']['ignored_tables']

    if gtid == 'pre_gtid_next':
        redis_con = redis_pool(REDIS_SETTINGS)
        pre_gtid = redis_con.hget('pre_gtid_next', redis_chanel)
        gtid = redis_con.hget('gtid_next', redis_chanel)
        print 'start at @pre_gtid_next \t', pre_gtid
        print 'gtid_next at > \t', gtid
        current_gtid = get_gtid(SMYSQL_SETTINGS)
        gtid_next = set_gtid(current_gtid, gtid)
    elif gtid == 'gtid_next':
        redis_con = redis_pool(REDIS_SETTINGS)
        pre_gtid = redis_con.hget('pre_gtid_next', redis_chanel)
        gtid = redis_con.hget('gtid_next', redis_chanel)
        print 'start at @gtid_next \t', gtid
        print 'pre_gtid_next at > \t', pre_gtid
        current_gtid = get_gtid(SMYSQL_SETTINGS)
        gtid_next = set_gtid(current_gtid, gtid)
    elif gtid == '':
        gtid_next = get_gtid(SMYSQL_SETTINGS)
        print gtid_next
    else:
        gtid_next = gtid
        for i in gtid.split(','):
            Gtid(i)

    main()
