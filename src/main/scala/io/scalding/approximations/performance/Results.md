After generating the Unique datasets, in order to load them into Hive / Impalla tables:

# COUNT UNIQUE

## Hive

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

## Hive Results

    select count(distinct key) from unique1M;   # 35 seconds
    select count(distinct key) from unique10M;  # 63 seconds
    select count(distinct key) from unique20M;  # 88 seconds
    select count(distinct key) from unique100M; # 194 seconds (12/1)
    select count(distinct key) from unique400M; # 545 seconds (45/1)
    
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
        
        