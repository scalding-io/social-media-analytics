After generating the Unique datasets, in order to load them into Hive / Impalla tables:

# Hive

CREATE TABLE unique1M   ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique10M  ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique20M  ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique100M ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique400M ( key string ) row format delimited fields terminated by '\t' stored as textfile;

load data inpath '/tmp/1M/'  into table unique1M;
load data inpath '/tmp/10M/' into table unique10M;
load data inpath '/tmp/20M/' into table unique20M;
load data inpath '/tmp/100M/' into table unique100M;
load data inpath '/tmp/400M/' into table unique400M;

## Measurements

    select count(distinct key) from unique1M;

1 Mapper - 1 Reducer = 35 seconds

    select count(distinct key) from unique10M;
    
3 Mapper - 1 Reducer = 63 seconds
    
    select count(distinct key) from unique20M;
    
4 Mapper - 1 Reducer = 88 seconds

    select count(distinct key) from unique100M;

12 Mapper - 1 Reducer = 194 seconds

    select count(distinct key) from unique400M;

45 Mapper - 1 Reducer = 545 seconds
