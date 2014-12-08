package io.scalding.approximations.BloomFilter

import com.twitter.scalding._
import com.twitter.algebird.{BF, BloomFilter}
import com.twitter.scalding.typed.TDsl._

/**
 * We generate 100.000 user ids ( 1 .. 100000 ) and add them into a BloomFilter
 * with a small estimation error. Then we execute membership queries on some ids
 *
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
class BFSimpleExample(args:Args) extends Job(args) {

  case class User(userID: String)

  val size = 100000
  val fpProb = 0.01

  implicit val bloomFilterMonoid = BloomFilter(size, fpProb)

  // Generate and add 100K ids into the (Bloom) filter
  val usersList = (1 to size).toList.map{ x => User(x.toString) }
  val usersBF = IterableSource[User](usersList)
    .map { user => bloomFilterMonoid.create(user.userID) }
    .sum

  // Display that BF can be queried
  usersBF
    .map { bf:BF =>
    println("BF contains 'ABCD' ? " + (if (bf.contains("ABCD").isTrue) "maybe" else "no"))
    println("BF contains 'EFGH' ? " + (if (bf.contains("EFGH").isTrue) "maybe" else "no"))
    println("BF contains '123'  ? " + (if (bf.contains("123") .isTrue) "maybe" else "no"))
    bf
  }
    .write( TypedCsv(args("output")) )

  // Serialize the BF
  usersBF
    .map { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
    .write( TypedCsv(args("serialized")) )


}