# pig-mysql 
fork from vertica/Vertica-Hadoop-Connector   
目前支持访问mysql 读取和写入

- store data using pig
```
register ~/tslpig-2.0.jar;
register ~/mysql-connector-java-5.1.41.jar;
a = load '/tmp/aaa' using PigStorage(',') as (id:chararray, age:int);
store a into '{testxx(insert into testxx values (?,?))}' using com.tsl.pig.MysqlStorer('host','test','3306','test','test');
```

- read data using pig
```
register ~/tslpig-1.0.jar;
register ~/mysql-connector-java-5.1.41.jar;
a = load 'sql://{select * from TESTXX }' using com.tsl.pig.VerticaLoader('HOST','TEST','3306','TEST','TEST');
dump a;
```
