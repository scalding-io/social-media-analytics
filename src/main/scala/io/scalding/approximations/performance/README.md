Performance evaluation of approximation algorithms **HLL** (HyperLogLog), **BF** (BloomFilter), **CMS** (CountMinSketch) 
using [Scalding](https://github.com/twitter/scalding) & [algebird](https://github.com/twitter/algebird/) versus Hive

The setup is a Cloudera 5.2.4 Hadoop cluster consisting of 7 `r3.8xlarge` amazon nodes, each offering:
 + 32 CPUs
 + 244 GB RAM
 + 3 * 1 TeraByte magnetic EBS volumes

The technologies evaluated are:
 + Scalding version = 0.13.1
 + Algebird version = 0.90
 + Hive version = 0.13.1-cdh5.2.4

# COUNTING CARDINALITY

This test is executed on synthetic data of 1 / 10 / 20 40 / 80 / 100 and 500 Million unique elements.
The generator used is [GenerateMillionKeys.scala](GenerateMillionKeys.scala). Data are pushed into HDFS,
and then queried through Hive and then through scalding/algebird's *HLL*. 

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

The above measurements are with using *12-bits* [ Where error rate is : 1.04 / sqrt (2^{bits}) ] ~ 1.6 % for HyperLogLog
By using *20-bits* the average error rate is ~ 0.1 % and the execution time increases marginally by just 1-2 seconds.

## Conclusions 

When counting cardinality on high volume data, think no more ! 
HyperLogLog is the actual winner for streaming and batch calculations.

# CALCULATING HISTOGRAMS

In this test, we will calculate the frequency table of the top-100 Wikipedia authors - using a 
[20 GByte](../../../../../../../datasets/wikipedia/README.md) Wikipedia dataset, containing more than 
400 M lines (403,802,472 lines) with 5,686,427 unique authors

Next, we will calculate the highest number of wikipedia edits / per second 

## Experiment - TopN on 5 million unique elements 

Getting the top-100 authors 

### Hive 0.13 results

|   |   |
|:--:|:--|
| HIVE Query     | `SELECT ContributorID, COUNT(ContributorID) AS CC  FROM wikipedia GROUP BY ContributorID ORDER BY CC DESC LIMIT 100` | 
| Execution Time | 77 seconds |
| Execution Plan | 74 Map - 19 Reduce - 4 Map - 1 Reduce |

### Scalding & Algebird

|       Scalding & Algebird       |    Execution Plan  | Execution Time |
| -------------------------------:| ------------------:| --------------:|
|       Wikipedia Top 10          | 148 Map - 1 Reduce |   67 seconds   |  
|       Wikipedia Top 100         | 148 Map - 1 Reduce |   72 seconds   |
|       Wikipedia Top 1000        | 148 Map - 1 Reduce |   73 seconds   |

## Experiment - TopN on 200 million unique elements

To simulate a larger experiment we will now calculate the histogram of the seconds with the highest writes/seconds

### Hive 0.13 results

|   |   |
|:--:|:--|
| HIVE Query     | `SELECT DateTime, COUNT(DateTime) AS CC FROM wikipedia GROUP BY DateTime ORDER BY CC DESC LIMIT 100` | 
| Execution Time | 230 seconds |
| Execution Plan | 74 Map - 19 Reduce - 4 Map - 1 Reduce |

### Scalding & Algebird

|       Scalding & Algebird       |    Execution Plan  | Execution Time |
| -------------------------------:| ------------------:| --------------:|
|    Wikipedia Top 100 seconds    | 148 Map - 1 Reduce |   78 seconds   |  

### Results

The results extracted are looking like this:

| Rank |        DateTime       | Count |
| ----:| ---------------------:| -----:|
|  0   | 2002-02-25T15:51:15Z  | 19491 |
|  1   | 2002-02-25T15:43:11Z  | 15153 |
|  2   | 2004-05-29T06:20:10Z  |   154 |
|  3   | 2004-06-02T08:06:20Z  |   149 |
|  4   | 2008-01-03T17:06:00Z  |   143 |
|  5   | 2008-01-03T17:09:29Z  |   136 |

## Experiment - Multiple Histograms on a single pass

To simulate how Scalding and algebird aggregations become increasingly important, as they offer the capability to calculate
multiple histograms on a single pass, we will calculate with scalding:

+ Frequency of Top-10 seconds regarding writes/seconds
+ Frequency of Top-100 authors
+ Frequency of Top-12 months
+ Frequency of Top-24 hours

in a single pass by executing:

    hadoop jar Social-Media-Analytics-assembly-1.0.jar com.twitter.scalding.Tool \
      io.scalding.approximations.CountMinSketch.WikipediaHistograms --hdfs \
      --input datasets/wikipedia/wikipedia-revisions

|     Scalding & Algebird     |    Execution Plan  | Execution Time |
| ---------------------------:| ------------------:| --------------:|
|  4 Histograms on Wikipedia  | 148 Map - 1 Reduce |  139 seconds   |  

## Conclusions

CMS (Count-Min-Sketch) offers the benefits:

+ As a monoid allows a single reducer to aggregate intermediate results - making it ideal for streaming application
+ Is performant once we are calculating histograms with 10s of Millions of unique items or more
+ Requires significantly small memory
+ Allows us to calculate 10s or even 100nds of histograms on a data set on a single pass

# USING BLOOM FILTERS

Look-ups can be **very** expensive at times. For example on HIVE the following lookup takes 23 seconds to be executed,
using 74 JVMs (map-tasks) on the cluster. 

    SELECT * FROM wikipedia WHERE ContributorID == 123456789;

....

    hadoop jar Social-Media-Analytics-assembly-1.0.jar com.twitter.scalding.Tool \
      io.scalding.approximations.BloomFilter.WikipediaBF --hdfs \
      --input datasets/wikipedia/wikipedia-revisions
 
....

The following implicit join notation is supported starting with Hive 0.13.0 

    SELECT * FROM unique1M     t1,  unique10M  t10 WHERE  t1.key ==  t10.key      # 68 sec
    SELECT * FROM unique10M   t10,  unique20M  t20 WHERE t10.key ==  t20.key      # 94 sec
    SELECT * FROM unique10M   t10, unique100M t100 WHERE t10.key == t100.key      # 115 sec       15 map / 4 reduce
    SELECT * FROM unique20M   t20, unique100M t100 WHERE t20.key == t100.key      # 120 sec       16 map / 4 reduce
    SELECT * FROM unique100M t100, unique100M t100a WHERE t100.key == t100a.key   # 275 sec       12 map / 3 reduce
     
    SELECT * FROM unique1M     t1,  unique1M  t1a WHERE  t1.key ==  t1a.key      # 41  sec
    SELECT * FROM unique10M   t10,  unique10M  t10a WHERE  t10.key ==  t10a.key  # 110 sec
        
# Notes

To reproduce the above performance measurements see [NOTES.md](NOTES.md)