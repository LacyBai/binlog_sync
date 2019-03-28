# MySQL binlog同步工具
## 应用场景

1. 业务的数据热迁移
2. 数据实时同步（同库或异库）
 
## 使用规则及注意事项

1. 源实例单库binlog订阅同步到目标实例指定库
2. 一个REDIS实例用来存储上一个gtid及下一个gtid
3. 启动脚本时不指定-g时，取当前会话的gtid
4. 无主键的表不建议使用，异常处理时可能无法保证数据一致性
5. binlog必须为row格式
6. 必须开启gtid

## 环境要求

1. Python 2.7+
2. Python modules: pymysqlreplication
3. 一个REDIS实例

## 使用方法

**帮助信息**
```bash
# ./binlog_sync.py -h
 Usage: binlog_sync.py [options]
 
 Options:
   -h, --help            show this help message and exit
   -c CONFIG, --config=CONFIG
                         The config file
   -i SERVER_ID, --server_id=SERVER_ID
                         The server-id
   -g GTID, --gtid=GTID  Start from where[pre_gtid_next|gtid_next]
```
**初次使用**
1. 导出需要同步的库表全量数据
```bash
mysqldump --single-transaction --master-data=2 -B hhz > hhz.sql
```
2. 获取gtid
```bash
grep -A5 'GTID_PURGED' hhz.sql
```
3. 将导出数据导入目标库
4. 配置文件，启动脚本添加-g指定获取的gtid （nohup启动，写异常日志）
        
**异常退出处理**
1. 通过异常退出日志处理错误
2. 根据情况加-g pre_gtid_next 或 gtid_next重新启动脚本