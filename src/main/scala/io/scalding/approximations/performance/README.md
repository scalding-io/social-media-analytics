Performance evaluation of approximation algorithms HLL (HyperLogLog), BF (BloomFilter), CMS (CountMinSketch) 
using Hive and Scalding & algebird.

The setup is a Cloudera 5.2.4 Hadoop cluster consisting of 7 `r3.8xlarge` amazon nodes, each offering:
 + 32 CPUs
 + 244 GB RAM
 + 3 * 1 TeraByte magnetic EBS volumes

The technologies evaluated are:
 + Hive version = 0.13.1-cdh5.2.4
 + Scalding version = 0.13.1
 + Algebird version = 0.90

# COUNTING CARDINALITY

This test is executed on synthetic data generated using [GenerateMillionKeys.scala](GenerateMillionKeys.scala) and pushed into HDFS. It demonstrates Hive execution time vs algebird's HLL. 

## Hive 0.13 results

|                  SQL QUERY                 |  Size  |  Map  |  Reduce  | Execution Time   |
| ------------------------------------------:| ------:| -----:| --------:| ----------------:|
| select count(distinct key) from unique1M;  |  30 MB |   1   |     1    |     33 seconds   |  
| select count(distinct key) from unique10M; | 305 MB |   3   |     1    |     63 seconds   |
| select count(distinct key) from unique20M; | 610 MB |   4   |     1    |     88 seconds   | 
| select count(distinct key) from unique40M; | 1,2 GB |   6   |     1    |    128 seconds   | 
| select count(distinct key) from unique80M; | 2,4 GB |  10   |     1    |    171 seconds   | 
| select count(distinct key) from unique100M;|   3 GB |  12   |     1    |    194 seconds   |   
| select count(distinct key) from unique500M;| 15,3GB |  57   |     1    |    833 seconds   |

## Scalding & Algebird 

|       Scalding & Algebird       |  Size  |  Map  |  Reduce  | Result 2% error  | Result 0.1% error | Execution Time | 
| -------------------------------:| ------:| -----:| --------:| ----------------:|------------------:|---------------:|
|          1MillionUnique         |  30MB  |   2   |    1     |        1,008,018 |         1,000,467 |   35 seconds   |
|         10MillionUnique         | 305MB  |   3   |    1     |        9,886,778 |         9,975,311 |   44 seconds   |
|         20MillionUnique         | 610MB  |   5   |    1     |       20,063,847 |        19,985,528 |   44 seconds   |
|         40MillionUnique         | 1,2GB  |  10   |    1     |       41,139,911 |        40,031,523 |   45 seconds   |
|         80MillionUnique         | 2,4GB  |  19   |    1     |       80,418,271 |        79,839,965 |   46 seconds   |
|        100MillionUnique         |   3GB  |  23   |    1     |       99,707,828 |       100,185,762 |   46 seconds   |
|        500MillionUnique         | 15,3GB | 114   |    1     |      495,467,613 |       500,631,225 |   52 seconds   |

The above measurements are with using *12-bits* [ Where error rate is : 1.04 / sqrt (2^{bits}) ] ~ 1.6 % 
By using *20-bits* the average error rate is ~ 0.1 % and the execution time increases marginally by 1-2 seconds.

# COUNTING Top-N

In this test, we will be calculating the frequency of the top-100 Wikipedia authors - using a ~ [20 GByte](../../../../../datasets/wikipedia/README.md) Wikipedia dataset, containing more than 400 M lines
(403,802,472 lines) and 5,686,427 unique authors

## Hive 0.13 results

|            HIVE Query           | Execution Plan | Execution Time | 
|:-------------------------------:| --------------:| --------------:| 
| SELECT ContributorID, COUNT(ContributorID) AS CC  FROM wikipedia GROUP BY ContributorID ORDER BY CC DESC LIMIT 10 | 74 Map - 19 Reduce - 4 Map - 1 Reduce |  77 seconds |
| ... LIMIT 100 | 74 Map - 19 Reduce - 4 Map - 1 Reduce |  77 seconds |
| ... LIMIT 100 | 74 Map - 19 Reduce - 4 Map - 1 Reduce |  77 seconds |

## Scalding & Algebird

|       Scalding & Algebird       |    Execution Plan  | Execution Time |
| -------------------------------:| ------------------:| --------------:|
|       Wikipedia Top 10          | 148 Map - 1 Reduce |   67 seconds   |  
|       Wikipedia Top 100         | 148 Map - 1 Reduce |   72 seconds   |
|       Wikipedia Top 1000        | 148 Map - 1 Reduce |   73 seconds   |

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
        
# NOTES

Generate data-sets:

    $ sbt clean assembly
    $ sbt "run-main io.scalding.approximations.performance.GenerateMillionKeys"
    $ sbt "run-main io.scalding.approximations.performance.GenerateMillionKeyValues"
    $ hadoop fs -mkdir datasets/
    $ hadoop fs -put datasets/* .     

Adding wikipedia dataset:

    $ wget --no-check-certificate http://tinyurl.com/n2lbe69 -O wikipedia-revisions.gz
    $ gunzip wikipedia-revisions.gz
    $ hadoop fs -put wikipedia-revisions datasets/
     
Executing Scalding (Count unique) :

    hadoop jar Social-Media-Analytics-assembly-1.0.jar com.twitter.scalding.Tool \
        io.scalding.approximations.performance.CountUniqueHLL --hdfs --input datasets/1MillionUnique

Executing HIVE (Count unique):

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
    
Executing Scalding (Top-10 Wikipedia):
    
    hadoop jar Social-Media-Analytics-assembly-1.0.jar com.twitter.scalding.Tool \
        io.scalding.approximations.CountMinSketch.WikipediaTopN --hdfs --input datasets/wikipedia/wikipedia-revisions --topN 10

Executing HIVE (Top-10 Wikipedia):

    CREATE TABLE wikipedia ( ContributorID INT, ContributorUserName STRING, RevisionID INT, DateTime STRING) row format delimited fields terminated by '\t' stored as textfile;
    load data inpath '/tmp/wikipedia/' into table wikipedia;

    select count(distinct ContributorID) from wikipedia;  # 74 Mappers - 1 Reducer #  Time: 66 seconds # Result = 5,686,427
 
    SELECT ContributorID, COUNT(ContributorID) AS CC 
    FROM wikipedia
    GROUP BY ContributorID
    ORDER BY CC DESC
    # 74 Map - 19 Reducer # 4 Map - 1 Reducer # Time: 82 seconds

    SELECT ContributorID, COUNT(ContributorID) AS CC 
    FROM wikipedia
    GROUP BY ContributorID
    ORDER BY CC DESC
    LIMIT 10
    # 74 Map - 19 Reducer - 4 Map - 1 Reducer # Time: 82 seconds


        Rank ContributorID  Count
         0	3455093	3762147
         1	1215485	3736115
         2	433328	3445761
         3	7328338	3107727
         4	6569922	2811455
         5	13286072	2294332
         6	4936590	1597294
         7	11292982	1577598
         8	4928500	1544177
         9	10996774	1447324
         10	205121	1281580
         11	7611264	1129643
         12	8066546	983709
         13	279219	965345
         14	82835	964309
         15	7320905	923242
        
    SELECT TOP 10 ContributorID, COUNT(ContributorID) AS Count
    FROM wikipedia
    GROUP BY ContributorID
    ORDER BY COUNT(ContributorID) DESC
