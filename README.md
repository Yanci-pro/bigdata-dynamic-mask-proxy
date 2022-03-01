# bigdata-dynamic-mask-proxy
基于netty实现的一个支持sql的大数据代理服务,用于处理不支持UDF的数据库和一些NOSQL非关系型数据库，可以用一个代理端口监控数据库的执行的内容，并对拦截的报文进行修改，记录日志或直接阻断会话


##redis测试脚本
```shell
set phoneA 13542156548
get phoneA


lpush phoneB 13542156897
lpush phoneB 13542114578
lrange phoneB 0 -1

zadd phoneC 1 13542159875
zadd phoneC 3 13684212456
zrange phoneC 0 -1 WITHSCORES


sadd phoneD 14569871235
sadd phoneD 15465421589
SMEMBERS phoneD


HSET phoneE zhangsan 13565421548    
HSET phoneE lisi 15698745215
HGET phoneE zhangsan
HGETALL phoneE  
```
