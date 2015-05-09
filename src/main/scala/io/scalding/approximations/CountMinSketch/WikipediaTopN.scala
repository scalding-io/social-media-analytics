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

  val input  = args.getOrElse("input" ,"datasets/wikipedia/wikipedia-revisions-sample.tsv") //
  val output = args.getOrElse("output","results/wikipedia-Top100")
  val topN   = args.getOrElse("topN","100").toInt

  // Construct a Count-Min Sketch monoid and bring in helping implicit conversions
  import CMSHasherImplicits._
  val cmsMonoid: TopNCMSMonoid[Long] =
    TopNCMS.monoid[Long](eps=0.01, delta=0.02, seed=(Math.random()*100).toInt, heavyHittersN = topN)

  val topNaggregator = TopNCMSAggregator(cmsMonoid)
    .composePrepare[Wikipedia](_.ContributorID)

  val wikiData: ValuePipe[TopCMS[Long]] = TypedPipe.from(TypedTsv[Wikipedia.WikipediaType](input))
    .map { Wikipedia.fromTuple }
    .aggregate(topNaggregator)

  wikiData
    .map { cms:TopCMS[Long] =>
      println(" + Total count in the CM sketch : " + cms.totalCount)
      println(" + Heavy Hitters : " + cms.heavyHitters.size)
      cms.heavyHitters.foreach( userid => { println("  - User ID : " + userid + " with estimated cardinality : " + cms.frequency(userid).estimate) } )
      io.scalding.approximations.Utils.serialize(cms)
    }
    .write(source.TypedSequenceFile(output))

}

object WikipediaTopNRunner extends App {
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner
  val timer = io.scalding.approximations.Utils.withTimeCalc("WikipediaTopN time") {
    ToolRunner.run(new Configuration, new Tool, (classOf[WikipediaTopN].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}
