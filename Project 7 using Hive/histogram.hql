drop table Histogram;
drop table Redtype;
drop table Greentype;
drop table Bluetype;
drop table Output;

create table Histogram(
red bigint,
green bigint,
blue bigint)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:P}' overwrite into table Histogram;                                                  
insert overwrite table Histogram select red+10000,green+20000,blue+30000 from Histogram;

create table Redtype(r int,red bigint,count bigint);
insert overwrite table Redtype select red/10000,red%1000,1 from Histogram;

create table Greentype(g int,green bigint,count bigint);
insert overwrite table Greentype select green/10000,green%1000,1 from Histogram;

create table Bluetype(b int,blue bigint,count bigint);
insert overwrite table Bluetype select blue/10000,blue%1000,1 from Histogram;

create table Output as select * from (
select * from Redtype
union all 
select * from Greentype
union all 
select * from Bluetype)unioned;

insert overwrite table Output select r,red,count(*) from Output group by r,red order by r,red;
select * from Output;

