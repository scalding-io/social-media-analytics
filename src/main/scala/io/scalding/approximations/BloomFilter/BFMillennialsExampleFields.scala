package io.scalding.approximations.BloomFilter

import com.twitter.scalding._
import com.twitter.algebird._

/**
 */
class BFMillennialsExampleFields(args:Args) extends Job(args) with FieldConversions {

  // Bloom Filter properties
  val size = 100000                                                             // Line 1
  val fpProb = 0.02D                                                            // Line 2

  // Our Bloom Filter mondoid
  implicit val bfMonoid = BloomFilter(size, fpProb)                             // Line 3

  val millennialsPipe = Tsv(args("input"), List('user, 'yearBorn)).read      // Line 4
    .filter('yearBorn) { yearBorn:Int => (yearBorn > 1980 & yearBorn < 2000) }  // Line 5
    .project('user)                                                             // Line 6
    .groupAll { group => group.toList[(String)]('user -> 'allUsers)}            // Line 7
    .mapTo('allUsers -> 'bloom) { ids:List[String] => bfMonoid.create(ids:_*) } // Line 8
    // Display some data to the screen
    .map('bloom->'report) {x:BF =>
      println( "5000 " + x.contains("5000").isTrue )
      println( "195000 " + x.contains("195000").isTrue)
      ""
    }

  // Serialize the BF
  millennialsPipe
    .mapTo('bloom -> 'serializes) { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
    .write( Tsv(args("serialized")) )

   millennialsPipe
    .write(Tsv(args("output")))

}