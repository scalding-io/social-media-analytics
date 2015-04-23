Performance evaluation of approximation algorithms : HyperLogLog / BloomFilter / CountMinSketch

# COUNTING CARDINALITY

Generate unique datasets using `GenerateMillionKeys.scala` and push files into HDFS.

The following test were executed **with** and **without HLL** on a Cloudera 5.2.4 Hadoop 
cluster consisting of 7-nodes with `r3.8xlarge` (each offering 32 CPUs, 244 GB RAM and three 
1 TB magnetic EBS volumes.

    $ sbt "run-main io.scalding.approximations.performance.GenerateMillionKeys"
    $ sbt "run-main io.scalding.approximations.performance.GenerateMillionKeyValues"
    $ hadoop fs -put datasets/* .     

## Hive

CREATE TABLE unique1M   ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique10M  ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique20M  ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique40M  ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique80M  ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique100M ( key string ) row format delimited fields terminated by '\t' stored as textfile;
CREATE TABLE unique500M ( key string ) row format delimited fields terminated by '\t' stored as textfile;

load data inpath '/tmp/1M/'   into table unique1M;
load data inpath '/tmp/10M/'  into table unique10M;
load data inpath '/tmp/20M/'  into table unique20M;
load data inpath '/tmp/40M/'  into table unique40M;
load data inpath '/tmp/80M/'  into table unique80M;
load data inpath '/tmp/100M/' into table unique100M;
load data inpath '/tmp/500M/' into table unique500M;

## Hive Results

    select count(distinct key) from unique1M;   # 35 seconds
    select count(distinct key) from unique10M;  # 63 seconds
    select count(distinct key) from unique20M;  # 88 seconds
    select count(distinct key) from unique40M;  #  seconds
    select count(distinct key) from unique80M;  #  seconds
    select count(distinct key) from unique100M; # 194 seconds (12/1)
    select count(distinct key) from unique500M; # 545 seconds (45/1)
    
## Scalding

hadoop jar Social-Media-Analytics-assembly-1.0.jar com.twitter.scalding.Tool \
    io.scalding.approximations.performance.CountUniqueHLL --hdfs --input datasets/1MillionUnique

    1MillionUnique                              # 35 seconds    ~result~  1,008,018
    10MillionUnique                             # 44 seconds    ~result~  9,886,778
    20MillionUnique                             # 44 seconds    ~result~ 20,063,847
    100MillionUnique                            # 46 seconds    ~result~ 97,049,737
    400MillionUnique                            # 51 seconds    ~result~ 97,049,737

# JOINING DATASETS

The following implicit join notation is supported starting with Hive 0.13.0 

    SELECT * FROM unique1M     t1,  unique10M  t10 WHERE  t1.key ==  t10.key      # 68 sec
    SELECT * FROM unique10M   t10,  unique20M  t20 WHERE t10.key ==  t20.key      # 94 sec
    SELECT * FROM unique10M   t10, unique100M t100 WHERE t10.key == t100.key      # 115 sec       15 map / 4 reduce
    SELECT * FROM unique20M   t20, unique100M t100 WHERE t20.key == t100.key      # 120 sec       16 map / 4 reduce
    SELECT * FROM unique100M t100, unique100M t100a WHERE t100.key == t100a.key   # 275 sec       12 map / 3 reduce
     
    SELECT * FROM unique1M     t1,  unique1M  t1a WHERE  t1.key ==  t1a.key      # 41  sec
    SELECT * FROM unique10M   t10,  unique10M  t10a WHERE  t10.key ==  t10a.key  # 110 sec

# BF join in Scalding

    JoinBigDatasetsBF           
    -----------------
    inputA                  inputB                  m/r         time 
    1MillionUnique          10MillionUnique         
    
    val inputA = args.getOrElse("inputA", "datasets/")
      val inputB = args.getOrElse("", "")
      val bfEntries = args.getOrElse("bfsize","1000000").toInt
        
        