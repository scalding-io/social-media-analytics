package io.scalding.approximations.BloomFilter.fields

import com.twitter.algebird._
import com.twitter.scalding._

/**
 * Scalding example of building a Bloom Filter using the Fields API
 *
 * @author Antonios Chalkiopoulos - http://scalding.io
 */
class BFExampleFields(args:Args) extends Job(args) with FieldConversions {

  // Bloom Filter properties
  val size = 100000    // 100 K unique elements
  val fpProb = 0.02D   // 2% estimation error

  implicit val bfMonoid: BloomFilterMonoid = BloomFilter(size, fpProb)    // Our Bloom Filter monoid

  val input=args.getOrElse("input","datasets/millennials/userdata.tsv")
  val output=args.getOrElse("output","results/BFExampleFields.tsv")
  val serialized=args.getOrElse("serialized","results/BFExampleFields.bf")

  val pipe = Tsv(input, List('username, 'yearBorn)).read
    .filter('yearBorn) { yearBorn:Int => yearBorn > 1980 & yearBorn < 2000 }
    .project('username)
    .groupAll { group =>
      group.foldLeft('username -> 'bloom)(bfMonoid.zero) {
        (bf: BF, username: String) => bf + username
      }
    }


  // Display some data to the screen
  pipe.map('bloom->'report) {x:BF =>
    println( "5000 " + x.contains("5000").isTrue )
    println( "195000 " + x.contains("195000").isTrue)
    ""
  }


  // Serialize the BF
  pipe
    .mapTo('bloom -> 'serialized) { bf:BF => new String(io.scalding.approximations.Utils.serialize(bf)) }
    .write( Tsv(serialized) )

   pipe
    .write(Tsv(output))

}