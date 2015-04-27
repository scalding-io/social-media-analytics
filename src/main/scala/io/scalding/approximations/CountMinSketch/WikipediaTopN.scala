package io.scalding.approximations.CountMinSketch

import com.twitter.algebird._
import com.twitter.scalding._
import io.scalding.approximations.model.Wikipedia

/**
 * This implementation uses algebird & scalding to calculate the TOP-100
 * authors - as per the wikipedia dataset using a CMS ( Count-Min-Sketch )
 * approximation algorithm
 *
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
class WikipediaTopN(args: Args) extends Job(args) {

  val input  = args.getOrElse("input" ,"datasets/wikipedia/wikipedia-revisions-sample.tsv")
  val output = args.getOrElse("output","results/wikipedia-Top100")
  val topN   = args.getOrElse("topN","100").toInt

  // Construct a Count-Min Sketch monoid and bring in helping implicit conversions
  import CMSHasherImplicits._
  implicit val cmsMonoid: TopNCMSMonoid[Long] =
    TopNCMS.monoid[Long](eps=0.01, delta=0.02, seed=(Math.random()*100).toInt, heavyHittersN = topN)

  val topNaggregator = TopNCMSAggregator(cmsMonoid)
    .composePrepare[Wikipedia](_.ContributorID)

  val wikiData = TypedPipe.from(TypedTsv[Wikipedia.WikipediaType](input))
    .map { Wikipedia.fromTuple }
    .aggregate(topNaggregator)
    .write(TypedTsv(output))

}
