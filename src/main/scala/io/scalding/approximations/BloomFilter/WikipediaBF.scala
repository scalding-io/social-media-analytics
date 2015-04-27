package io.scalding.approximations.BloomFilter

import com.twitter.algebird._
import com.twitter.scalding._
import io.scalding.approximations.model.Wikipedia

/**
 * Generate a BF containing all the unique authors of our wikipedia dataset
 * There are roughly 5 Million of them with a probability of 0.1 %
 *
 * @author Antonios Chalkiopoulos - http://scalding.io
 */
class WikipediaBF(args:Args) extends Job(args) {

  val input  = args.getOrElse("input" ,"datasets/wikipedia/wikipedia-revisions-sample.tsv")
  val serialized = args.getOrElse("serialized","results/wikipedia-authors-BF")

  val bloomFilterMonoid = BloomFilter(numEntries = 5000000 , fpProb = 0.001)

  // BF aggregator
  val bfAggregator = BloomFilterAggregator
    .apply(bloomFilterMonoid)
    .composePrepare[Wikipedia](_.ContributorID.toString)

  val wikiData = TypedPipe.from(TypedTsv[Wikipedia.WikipediaType](input))
    .map { Wikipedia.fromTuple }
    .aggregate(bfAggregator)
    .map { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
    .write( TypedCsv(serialized) )

}