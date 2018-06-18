# esTransfer
> elasticsearch data transfer between clusters. Using elasticsearch-5.6.0 version.

##配置示例如下

##数据源es配置
app.es.source.esHosts=127.0.0.1:9200
app.es.source.esUserName=***
app.es.source.esPassword=***
app.es.source.index=index
app.es.source.type=type
###数据源指定字段，*或空表示全部字段
app.es.source.properties=
###查询过滤条件，填写query内的条件，json格式，eg:{"match_all":{}}。无特殊需求则置空
app.es.source.query=

##目标数据es配置
app.es.target.esHosts=127.0.0.2:9200
app.es.target.esUserName=***
app.es.target.esPassword=***
app.es.target.index=index
app.es.target.type=type

##其他配置
###size默认为1000
es.size=1000
