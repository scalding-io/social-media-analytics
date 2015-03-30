# Social Media Data Mining and Analytics

[![Build Status](https://travis-ci.org/scalding-io/social-media-analytics.svg?branch=master)](https://travis-ci.org/scalding-io/social-media-analytics)

This is the expected title for the book from Wiley that will be available March 2015 and authored by Gabor Szabo, Oscar Boykin and Antonios Chalkiopoulos

<p align="center">
  <a href="http://eu.wiley.com/WileyCDA/WileyTitle/productCd-1118824857.html" target="_blank"><img src="http://media.wiley.com/product_data/coverImage300/57/11188248/1118824857.jpg"/></a>
</p>

In this repository you can find code from <a href="https://github.com/Antwnis">Antonios Chalkiopoulos</a> and also <a href="https://github.com/galarragas">Stefano Galarragas</a> 
about examples of using probabilistic data structures with Scalding.

You know we are talking about:

* HyperLogLog
* Bloom Filters
* Count-Min sketch

We will be using sample data from wikipedia, stackexchange and of course we will demonstrate how to produce computer generate data with Scalding, to control the size, skeweness and type of data.

Overall i hope that together with the book, this code repository can bring some more light into the usefulness of HLL, BF, CM sketches in modern analytics either in batch mode (MapReduce) or in a different execution fabrics.

# Quickstart
  
    $ git clone https://github.com/scalding-io/social-media-analytics.git
    $ cd social-media-analytics/
    $ sbt "run-main io.scalding.approximations.ExamplesRunner"

and to run the scalding tests

    $ sbt clean test

to generate a Hadoop executable JAR

    $ sbt clean assembly

to run the Hadoop JAR file

    $ hadoop fs -put datasets
    $ hadoop jar Social-Media-Analytics-assembly-1.0.jar com.twitter.scalding.Tool \
             io.scalding.approximations.BloomFilter.BFStackExchangeUsersPostExtractor --hdfs \
             --posts datasets/stackexchange/posts.tsv --users datasets/stackexchange/users.tsv \
             --minAge 18 --maxAge 21 --output results/BF-StackExchangeUsersPost
    $ hadoop fs -ls results/BF-StackExchangeUsersPost

## FAQ

1. How can i view the flow planner ?

Run with `--tool.graph` This will not execute a Job - and instead will create a .dot file

    $ hadoop jar Social-Media-Analytics-assembly-1.0.jar com.twitter.scalding.Tool \
             io.scalding.approximations.BloomFilter.BFStackExchangeUsersPostExtractor --hdfs \
             --posts datasets/stackexchange/posts.tsv --users datasets/stackexchange/users.tsv \
             --minAge 18 --maxAge 21 --output results/BF-StackExchangeUsersPost \
             --tool.graph

