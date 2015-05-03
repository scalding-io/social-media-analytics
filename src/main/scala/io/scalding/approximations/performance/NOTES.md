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

....

The following implicit join notation is supported starting with Hive 0.13.0 

    SELECT * FROM unique1M     t1,  unique10M  t10 WHERE  t1.key ==  t10.key      # 68 sec
    SELECT * FROM unique10M   t10,  unique20M  t20 WHERE t10.key ==  t20.key      # 94 sec
    SELECT * FROM unique10M   t10, unique100M t100 WHERE t10.key == t100.key      # 115 sec       15 map / 4 reduce
    SELECT * FROM unique20M   t20, unique100M t100 WHERE t20.key == t100.key      # 120 sec       16 map / 4 reduce
    SELECT * FROM unique100M t100, unique100M t100a WHERE t100.key == t100a.key   # 275 sec       12 map / 3 reduce
     
    SELECT * FROM unique1M     t1,  unique1M  t1a WHERE  t1.key ==  t1a.key      # 41  sec
    SELECT * FROM unique10M   t10,  unique10M  t10a WHERE  t10.key ==  t10a.key  # 110 sec