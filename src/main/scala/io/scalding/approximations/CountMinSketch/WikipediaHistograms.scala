package io.scalding.approximations.CountMinSketch

import com.twitter.algebird._
import com.twitter.scalding._
import io.scalding.approximations.model.Wikipedia

import scala.math.BigInt

/**
 * This implementation uses algebird & scalding to calculate multiple histograms on a dataset using CMS
 *
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
class WikipediaHistograms(args: Args) extends Job(args) {

  val input  = args.getOrElse("input" ,"datasets/wikipedia/wikipedia-revisions-sample.tsv")
  val output = args.getOrElse("output","results/wikipedia-multiHistograms")
  val seed   = (Math.random()*100).toInt

  import CMSHasherImplicits._
  // Top-10 seconds with most writes/seconds
  val top10qpsMonoid= TopNCMS.monoid[BigInt](0.01,0.02,seed, heavyHittersN = 10)
  val top10qps = TopNCMSAggregator(top10qpsMonoid) .composePrepare[Wikipedia](x => BigInt( x.DateTime.getBytes ))
  // Top-100 authors
  val top100authorsMonoid = TopNCMS.monoid[Long](0.01, 0.02,seed, heavyHittersN = 100)
  val top100authors = TopNCMSAggregator(top100authorsMonoid).composePrepare[Wikipedia](_.ContributorID)
  // Top-12 months
  val topMonthsMonoid = TopNCMS.monoid[Long](0.01,0.02,seed,heavyHittersN = 12)
  val top12Months = TopNCMSAggregator(topMonthsMonoid).composePrepare[Wikipedia](x => x.DateTime.substring(5,7).toLong)
  // Top-24 hours
  val top24HoursMonoid = TopNCMS.monoid[Long](0.01,0.02,seed,heavyHittersN = 12)
  val top24Hours = TopNCMSAggregator(top24HoursMonoid).composePrepare[Wikipedia](x => x.DateTime.substring(8,10).toLong)

  val wikiData = TypedPipe.from(TypedTsv[Wikipedia.WikipediaType](input))
    .map { Wikipedia.fromTuple }
    .aggregate(GeneratedTupleAggregator.from4(top10qps, top100authors, top12Months,top24Hours))
    .write(TypedTsv(output))

}
