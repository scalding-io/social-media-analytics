Social Media Data Mining and Analytics
======================================

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

We will be using as sample data, both from wikipedia, stackexchange and of course we will demonstrate how to produce computer generate data with scalding, to have the perfect skeweness and type of data.

Overall i hope that together with the book, this code repository can bring some more light into the usefullness of HLL, BF, CM sketches in modern analytics either in batch mode (MapReduce) or in a different execution fabric.

Quickstart
==========
  
    $ git clone https://github.com/scalding-io/social-media-analytics.git
    $ cd social-media-analytics/
    $ gradle run

and to run the scalding tests

    $ gradle test

to generate a Hadoop executable JAR

    $ gradle clean jar hadoopJar
