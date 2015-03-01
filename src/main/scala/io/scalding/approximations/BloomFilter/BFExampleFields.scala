package io.scalding.approximations.BloomFilter

import com.twitter.scalding._
import com.twitter.algebird._

/**
 * Scalding example of interacting with Bloom Filters using the Fields API
 */
class BFExampleFields(args:Args) extends Job(args) with FieldConversions {

  // Bloom Filter properties
  val size = 100000    // 100 K unique elements
  val fpProb = 0.02D   // 2% estimation error

  implicit val bfMonoid = BloomFilter(size, fpProb)    // Our Bloom Filter monoid

  val input=args.getOrElse("input","datasets/millennials/userdata.tsv")
  val output=args.getOrElse("output","results/BFExampleFields.tsv")
  val serialized=args.getOrElse("serialized","results/BFExampleFields.bf")

  val millennialsPipe = Tsv(input, List('username, 'yearBorn)).read
    .filter('yearBorn) { yearBorn:Int => (yearBorn > 1980 & yearBorn < 2000) }
    .project('username)
    .groupAll { group => group.toList[(String)]('username -> 'allUsers)}            // Line 7
    .mapTo('allUsers -> 'bloom) { ids:List[String] => bfMonoid.create(ids:_*) } // Line 8
    // Display some data to the screen
    .map('bloom->'report) {x:BF =>
      println( "5000 " + x.contains("5000").isTrue )
      println( "195000 " + x.contains("195000").isTrue)
      ""
    }

  // Serialize the BF
  millennialsPipe
    .mapTo('bloom -> 'serialized) { bf:BF => new String(io.scalding.approximations.Utils.serialize(bf)) }
    .write( Tsv(serialized) )

   millennialsPipe
    .write(Tsv(output))

}