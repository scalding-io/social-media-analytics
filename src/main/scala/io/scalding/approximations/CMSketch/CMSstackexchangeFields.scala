package io.scalding.approximations.CMSketch

import com.twitter.algebird._
import com.twitter.scalding._

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
class CMSstackexchangeFields(args: Args) extends Job(args) {

  // The input schema of our data-set
  val schema = List('ID, 'PostTypeID, 'ParentID, 'OwnerUserID, 'CreationDate, 'ViewCount, 'FavoriteCount, 'Tags, 'Keywords)

  // Construct a Count-Min Sketch monoid and initialize an empty sketch
  val eps: Double = 0.01
  val delta: Double = 0.02
  val seed: Int = (Math.random()*100).toInt

  implicit val cmsketchMonoid = new CountMinSketchMonoid(eps, delta, seed)

  val stackExchangePosts = Tsv(args("input"),schema).read
    .mapTo('OwnerUserID-> 'cms) { x:Long => cmsketchMonoid.create(x) }
    .groupAll { group =>
      group.reduce('cms -> 'cms) { (left:CMS,right:CMS) => left ++ right }
    }
    .write(Tsv(args("output")))

}