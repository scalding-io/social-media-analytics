package io.scalding.approximations.performance

import com.twitter.algebird._
import com.twitter.scalding._
import io.scalding.approximations.model.Wikipedia

class ComposingAggregators(args: Args) extends Job(args) {

  val input  = args.getOrElse("input" ,"datasets/wikipedia/wikipedia-revisions-sample.tsv")
  val output = args.getOrElse("output","results/composedAggregator")
  val topN   = args.getOrElse("topN","100").toInt
  val BFsize = args.getOrElse("BFsize","200000").toInt

  // HLL count with 1.6 % accuracy the unique number of wikipedia authors
  implicit def long2Bytes(num:Long): Array[Byte] = num.toString.getBytes
  val uniqueHLL = HyperLogLogAggregator
    .sizeAggregator(12)
    .composePrepare[Wikipedia](_.ContributorID)

  // CMS calculate the top-N authors of wikipedia
  import CMSHasherImplicits._
  implicit val cmsMonoid: TopNCMSMonoid[Long] =
    TopNCMS.monoid[Long](eps=0.01, delta=0.02, seed=(Math.random()*100).toInt, heavyHittersN = topN)
  val topNaggregator = TopNCMSAggregator(cmsMonoid)
    .composePrepare[Wikipedia](_.ContributorID)

  // BF generate a filter
  implicit val bloomFilterMonoid = BloomFilter(numEntries = BFsize , fpProb = 0.02)
  val bfAggregator = BloomFilterAggregator
    .apply(bloomFilterMonoid)
    .composePrepare[Wikipedia](_.ContributorID.toString)

  // Calculate HLL - CMS - BF in a single pass
  val multiAggregator = GeneratedTupleAggregator
    .from3(uniqueHLL, topNaggregator, bfAggregator)

  // Read in data and calculate approximation data structures
  val wikiData = TypedPipe.from(TypedTsv[Wikipedia.WikipediaType](input))
    .map { Wikipedia.fromTuple }
    .aggregate(multiAggregator)
    .debug
    .write(TypedCsv(output))

}
