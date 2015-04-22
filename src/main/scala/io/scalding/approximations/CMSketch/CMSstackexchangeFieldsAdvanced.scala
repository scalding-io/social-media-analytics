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
class CMSstackexchangeFieldsAdvanced(args: Args) extends Job(args) {

  // The input schema of our data-set
  val schema = List('ID, 'PostTypeID, 'ParentID, 'OwnerUserID, 'CreationDate, 'ViewCount, 'FavoriteCount, 'Tags, 'Keywords)

  // Take in as arguments the location of the input/output files
  val inputFiles = args("input")
  val outputFile = args("output")

  // Construct a Count-Min Sketch monoid and initialize an empty sketch
  val eps: Double = 0.01
  val delta: Double = 0.02
  val seed: Int = (Math.random()*100).toInt

  import CMSHasherImplicits._
  implicit val cmsketchMonoid =
    TopNCMS.monoid[Long](eps=0.01, delta=0.02, seed=(Math.random()*100).toInt, heavyHittersN = 100)

//  implicit val cmsketchMonoid = new CMSMonoid[Long](eps, delta, seed)
//  implicit val cmsketchMonoid = new CountMinSketchMonoid(eps, delta, seed)
  val sketch:Option[TopCMS[Long]] = None

  val stackExchangePosts = Tsv(inputFiles,schema).read
    .project('OwnerUserID)
    .insert('sketch, sketch)
    // group all to generate a single stream of tuples
    .groupAll { group =>
      // fold left the stream of tuples passing in as initial paramters
      // to the function the `monoid` and an empty `sketch`
      group.foldLeft(('OwnerUserID, 'sketch)->('OwnerUserID, 'sketch)) ( (cmsketchMonoid, sketch) ) { cmSketch }
    }
    .discard('OwnerUserID)
    .map('sketch -> 'sketch) { s: Option[TopCMS[Long]] =>
      s match {
        case Some(sk) =>
          println(" + Total count in the CM sketch : " + sk.totalCount)
          println(" + Heavy Hitters : " + sk.heavyHitters.size)
          sk.heavyHitters.foreach( userid => { println("  - User ID : " + userid + " with estimated cardinality : " + sk.frequency(userid).estimate) } )
        case _ => println("No information to display")
      }
      s
    }
    .write(Tsv(outputFile))

  /*
   * The foldLeft function that accumulates the sketch information over a stream of user IDs
   */
  def cmSketch(accumulator:(TopNCMSMonoid[Long],Option[TopCMS[Long]]), curr:(Long, Option[TopCMS[Long]])) : (TopNCMSMonoid[Long],Option[TopCMS[Long]]) = {
    // Get the CMS monoid and item from the accumulator and the user id as the first element of the current line
    val (cmsketchMonoid,existingSketch) = accumulator
    val userid = curr._1
    // Create a sketch out of a single item
    val additionalSketch = cmsketchMonoid.create(userid)
    // Augment the current sketch by ++ with the existing sketch
    val newSketch = if( existingSketch.isDefined )  Some(existingSketch.get ++ additionalSketch) else Some(additionalSketch)
    // Return the monoid and the sketch
    (cmsketchMonoid, newSketch)
  }
  
}