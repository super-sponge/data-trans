## data exchange tool
    本工具通过连接oracle视图生成用户datax 调用的 job json文件
    
## 编译与部署
    mvn package
    cd target
    tar -zxf data-meta-1.0-package.tar.gz 
    
## 工具介绍
### oracle 表信息导出工具
    ./ora_utils.sh -h
    usage: May Options
     -f <arg>   output json path
     -h         Help description
     -j <arg>   jdbc connect string
     -p <arg>   password
     -t <arg>   table name　（当不指定此参数为导出此用户所有表结构信息)
     -u <arg>   user
    ./ora_utils.sh  通过输入oracle信息把oracle的表的列信息导出导出到(-f 参数指定) 指定文件，其导出文件的格式为
    表名称|列名称|原来类型|Hive类型
    EMP,EMPNO,NUMBER,BIGINT
    EMP,ENAME,VARCHAR2,STRING
### datax 配置文件生成工具
    ./datax_job.sh  -h
    usage: May Options
     -c <arg>   compress type NONE|SNAPPY default[NONE]  (指定存放到hdfs的压缩格式)
     -d <arg>   delimiter default[\t]  (指定存储格式为text时,每行的分隔符号)
     -e <arg>   hive databases path default[/apps/hive/warehouse]  (hive　数据库目录)
     -f <arg>   hdfs defaultFS hdfs://localhost:8020
     -h         Help description
     -j <arg>   jdbc connect string  (oracle jdbc 连接)
     -m <arg>   meta info file  (table metainfo 文件）
     -n <arg>   channel num default[2]　　（抽取channel 数量)
     -o <arg>   output json path (生成的json 文件路径, 必须提前创建)
     -p <arg>   password (oracle 密码)
     -t <arg>   fileType orc|text default[orc]
     -u <arg>   user (oracle 用户)
     -w <arg>   writeMode type append|nonConflict default[orc] (文件写入方式)
     
     此工具根据输入的oracle表结构信息，生成datax的job文件，同时生成hive建表语句.  
   
## 使用案例
    导出所有表信息到 all.txt 文件
    ./ora_utils.sh -j "jdbc:oracle:thin:@10.0.8.156:1521:orcl" -u scott -p tiger -f all.txt
    导出emp表到emp.txt 文件
    ./ora_utils.sh -j "jdbc:oracle:thin:@10.0.8.156:1521:orcl" -u scott -p tiger -t emp -f emp.txt
    
    根据all.txt 文件,生成相应配置文件到/tmp/data 目录
    ./datax_job.sh -j "jdbc:oracle:thin:@10.0.8.156:1521:orcl" -u scott -p tiger -m all.txt -f "hdfs://sdc1.sefon.com:8020" -o /tmp/data


