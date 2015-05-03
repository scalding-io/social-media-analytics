package io.scalding.approximations.BloomFilter

import com.twitter.algebird._
import com.twitter.scalding._
import com.twitter.scalding.source.TypedSequenceFile

/**
 * We generate 100.000 user ids ( 1 .. 100000 ) and add them into a BloomFilter
 * with a small estimation error. Then we execute membership queries on some ids
 *
 * @author Antonios Chalkiopoulos - http://scalding.io
 */
class SimpleBFAggregator(args:Args) extends Job(args) {

  val bloomFilterMonoid = BloomFilter(numEntries = 100000 , fpProb = 0.02)

  // BF aggregator
  val bfAggregator = BloomFilterAggregator
    .apply(bloomFilterMonoid)
    .composePrepare[SimpleUser](_.userID)

  // Generate and add 100K ids into the (Bloom) filter
  val usersList = (1 to 100000).toList.map{ x => SimpleUser(x.toString) }
  val usersBF = typed.IterablePipe[SimpleUser](usersList)
    .aggregate(bfAggregator)

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
  // Note: Serialization adds a few more seconds to execution time
  // So expect 4 seconds to generate the BF and 40 more if you choose to serialize it
  usersBF
    .map { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
    .write( TypedSequenceFile(args.getOrElse("serialized","results/SimpleBFAggregator-serialized")) )

}

object SimpleBFAggregatorRunner extends App {
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner
  val timer = io.scalding.approximations.Utils.withTimeCalc("SimpleBFAggregatorRunner") {
    ToolRunner.run(new Configuration, new Tool, (classOf[SimpleBFAggregator].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Running BF synthetic-data example took $timer msec in –-local mode")
}