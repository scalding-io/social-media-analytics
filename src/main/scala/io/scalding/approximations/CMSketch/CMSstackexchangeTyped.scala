package io.scalding.approximations.CMSketch

import com.twitter.algebird._
import com.twitter.scalding._
import com.twitter.scalding.typed.TDsl._

/**
 * CMSketch example application: It processes post data from www.stackexchange.com
 * You can use as an example the data in `datasets/stackexchange/posts.tsv`,
 * which is a 22 MByte data-set with 9 columns of data and 42.150 lines of data.
 *
 * We will stream all that user-id of that data-set through a Count-Min Sketch to generate a
 * `frequency table`.
 *
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
class CMSstackexchangeTyped(args: Args) extends Job(args) {

  // Construct a Count-min Sketch monoid and initialize an empty sketch

  import CMSHasherImplicits._
  implicit val cmsketchMonoid =
    TopNCMS.monoid[Long](eps=0.01, delta=0.02, seed=(Math.random()*100).toInt, heavyHittersN = 100)

  val stackExchangeCMS = TypedTsv[(String,String,String,Long,String,String,String,String,String)](args("input")).read
    .toTypedPipe[(String,String,String,Long,String,String,String,String,String)]('*)
    .map{ case (_,_,_,owner,_,_,_,_,_) => cmsketchMonoid.create(owner) }
    .sum

  // Display histogram info from CMS
  stackExchangeCMS
    .map { cms:TopCMS[Long] =>
       println(" + Total count in the CM sketch : " + cms.totalCount)
       println(" + Heavy Hitters : " + cms.heavyHitters.size)
       cms.heavyHitters.foreach( userid => { println("  - User ID : " + userid + " with estimated cardinality : " + cms.frequency(userid).estimate) } )
       cms
    }
    .write( TypedCsv(args("output")) )

  // Serialize the CMS
  stackExchangeCMS
    .map { cms:TopCMS[Long] => new String(io.scalding.approximations.Utils.serialize(cms)) }
    .write( TypedCsv(args("serialized")) )

}