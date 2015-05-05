package io.scalding.approximations.BloomFilter

import com.twitter.algebird._
import com.twitter.scalding._
import com.twitter.scalding.source.TypedSequenceFile
import io.scalding.approximations.model.Wikipedia

/**
 * Generate a BF per month - containing all the unique authors of Wikipedia that were active during that month
 * There are roughly 5 Million authors in dataset
 *
 * @author Antonios Chalkiopoulos - http://scalding.io
 */
class WikipediaBF(args:Args) extends Job(args) {

  val input  = args.getOrElse("input" ,"datasets/wikipedia/wikipedia-revisions-sample.tsv") //
  val serialized = args.getOrElse("serialized","results/wikipedia-per-month-BF-serialized")

  // We don't know a priori how big the filters need to be
  // So let's use HLL to get an approximate count
  val hllAggregator = HyperLogLogAggregator
    .sizeAggregator(12)
    .composePrepare[Wikipedia](_.ContributorID.toString.getBytes("UTF-8"))

  val wikiHLL = TypedPipe.from(TypedTsv[Wikipedia.WikipediaType](input))
    .map { Wikipedia.fromTuple }
    .map { wiki => wiki.copy(DateTime = wiki.DateTime.substring(0,7)) } // extract YYYY-MM
    .groupBy { wiki => wiki.DateTime }
    .aggregate(hllAggregator)
    .mapped
    .map{ case (key:String,value:HLL) => (key,value.approximateSize.estimate) }
    .groupBy { _._1 }
    .sum
    .values
  // Example output is =>    Key = 2011-02 , Value = 149804

  // Now that we know how large each group is, we will instantiate one BloomFilterMonoid per group
  // Also as HLL is an approximate count we will add 10 % to the size of the filters (* 1.1)
  val BFilters =
    wikiHLL.map {
      case (key,value) => (key.substring(0,7), BloomFilter(numEntries = (value*1.1).toInt , fpProb = 0.02D) )
    }
  // Example output is =>   Key = 2011-02 , Value = BloomFilterMonoid(164784,0.02)

  // All the above calculations have been done JUST for creating optimal sized BF
  // So now, we will read in all the data, group by month and JOIN them with the initialized BloomFilters
  val wikiData = TypedPipe.from(TypedTsv[Wikipedia.WikipediaType](input))
    .map { Wikipedia.fromTuple }
    .map { wiki => wiki.copy(DateTime = wiki.DateTime.substring(0,7)) } // extract YYYY-MM
    .groupBy { wiki => wiki.DateTime }
    .join { BFilters }

  // All that is left to happen is to create a BF for every item in the group and then UNION them together
  val result = wikiData
    .mapValues { case (wiki, bf) =>
      bf.create(wiki.ContributorID + "")
    }
    .reduce{ (left,right) => left ++ right }
    .mapValues { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
    .toTypedPipe
    .write(TypedSequenceFile("results/wikipedia-per-month-BF"))

    // And if you want to write ONE file per month - chose an option:
    // .toPipe('month, 'bfserialized)
    // .write(TemplatedSequenceFile("results/wikipedia-per-month-BF/","month-%s",'month))
    // .write(PartitionedSequenceFile("results/wikipedia-per-month-BF/",pathFields = 'month))

  // Also let's store the HLL results
  wikiHLL
    .write(TypedTsv("results/wikipedia-per-month-HLL.tsv") )

}

object WikipediaBFRunner extends App {
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner
  val timer = io.scalding.approximations.Utils.withTimeCalc("WikipediaBF time") {
    ToolRunner.run(new Configuration, new Tool, (classOf[WikipediaBF].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}

/** Results of HLL - Execution time: 1,372,025 msec ~ 24 minutes on a MacBookPro --local

2007-10,166996
2003-01,981
2012-05,141356
2008-11,152595
2009-05,157920
2010-01,157696
2004-05,6350
2001-09,211
2003-06,1418
2001-02,21
2008-05,165681
2012-09,125894
2009-01,157483
2002-01,192
2009-11,159021
2005-06,25026
2010-04,154992
2004-01,3133
2006-02,77046
2011-02,149804
2004-12,14129
2002-12,666
2012-02,144421
2005-02,15250
2007-04,187151
2010-08,133454
2013-01,137526
2006-06,110596
2003-05,1242
2009-09,148815
2002-09,528
2007-08,159038
2001-11,216
2013-06,118390
2011-12,136025
2012-10,133134
2001-05,36
2010-12,127857
2011-08,147036
2001-03,45
2013-04,126339
2009-02,156397
2003-12,2600
2001-08,117
2012-04,136469
2009-04,152883
2004-11,13623
2005-05,23861
2010-03,161559
2003-07,1800
2008-04,174184
2002-06,279
2004-04,5746
2009-12,147700
2012-08,130004
2002-04,237
2006-10,145966
2003-04,897
2007-07,162248
2002-11,594
2011-04,143488
2013-07,127391
2007-11,161548
2012-03,144520
2001-12,249
2004-07,7806
2011-07,145639
2007-03,198309
2001-04,32
2005-12,62300
2008-08,148193
2009-08,145785
2005-01,14633
2010-07,133337
2008-01,169771
2005-10,40307
2006-05,108635
2010-02,147269
2006-04,97928
2005-08,35970
2008-03,181257
2011-06,144215
2004-10,11853
2006-11,159335
2006-09,134422
2002-07,313
2009-03,168878
2005-04,21652
2003-08,1992
2007-01,174574
2003-11,2514
2008-12,146775
2012-07,134653
2009-07,145757
2002-03,275
2004-03,5602
2006-01,69737
2004-08,8816
2010-10,135862
2007-12,151433
2011-03,161782
2002-10,610
2007-06,159144
2007-02,176912
2008-02,165678
2010-06,141161
2011-10,143617
2013-08,35616
2001-07,114
2006-12,156938
2013-03,135337
2006-08,137256
2008-07,149341
2003-03,930
2005-07,31325
2012-11,133406
2008-06,151126
2002-02,291
2008-10,157479
2005-11,43552
2005-09,35132
2006-03,93270
2002-08,417
2011-05,146364
2010-11,133634
2001-10,200
2004-06,7361
2009-06,153742
2012-06,127486
2011-01,147708
2010-05,152890
2008-09,152471
2004-02,4230
2003-10,2358
2003-09,2007
2002-05,227
2010-09,131116
2013-02,131526
2001-01,21
2007-05,180177
2009-10,154551
2006-07,119011
2005-03,18591
2012-01,145807
2003-02,989
2012-12,124838
2007-09,161844
2004-09,10315
2011-09,145536
2013-05,130017
2011-11,143233
2001-06,40

*/
