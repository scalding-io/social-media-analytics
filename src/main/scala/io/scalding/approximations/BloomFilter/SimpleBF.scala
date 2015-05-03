package io.scalding.approximations.BloomFilter

import com.twitter.algebird._
import com.twitter.scalding._

/**
 * We generate 100.000 user ids ( 1 .. 100000 ) and add them into a BloomFilter
 * with a small estimation error. Then we execute membership queries on some ids
 *
 * @author Antonios Chalkiopoulos - http://scalding.io
 */
class SimpleBF(args:Args) extends Job(args) {

  // Implicit is required for `sum` operation
  implicit val bloomFilterMonoid = BloomFilter(numEntries = 1000000 , fpProb = 0.02D)

  // Generate and add 100K ids into the (Bloom) filter
  val usersList = (1 to 100000).toList.map{ x => SimpleUser(x.toString) }
  val usersBF = typed.IterablePipe[SimpleUser](usersList)
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
    .write( TypedCsv(args.getOrElse("output","results/SimpleBFAggregator")) )

  // Serialize the BF
  usersBF
    .map { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
    .write( TypedCsv(args.getOrElse("serialized","results/SimpleBFAggregator-serialized")) )

}

object SimpleBFRunner extends App {
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner
  val timer = io.scalding.approximations.Utils.withTimeCalc("BF time") {
    ToolRunner.run(new Configuration, new Tool, (classOf[SimpleBF].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}