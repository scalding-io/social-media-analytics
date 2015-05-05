package io.scalding.approximations.BloomFilter

import com.twitter.scalding._
import com.twitter.algebird._

/**
 * In `SimpleBFAggregator` we generate a BF and serialize it into a file at the location `resuts/SimpleBFAggregator-serialized/part-00000`
 *
 * In this scalding job - we will read that file, deserialize it and use it
 */
class PrecomputedBF(args:Args) extends Job(args) {

  val serializedBF = args.getOrElse("serialized", "results/SimpleBFAggregator-serialized/part-00000")
  val BF = TypedPipe.from(source.TypedSequenceFile(serializedBF))
    .map {
      serialized:Array[Byte] => io.scalding.approximations.Utils.deserialize[BF](serialized)
    }

  val itemsPipe = typed.IterablePipe[String](List( "ABCD", "EFGH", "123" ))

  BF
    .cross(itemsPipe)
    .map { case (bf: BF, item: String) =>
      val existsInBF = bf.contains(item)
      println(s"Item $item exists in BF : $existsInBF")
      (item, existsInBF.isTrue)
    }
    .write( TypedTsv(args.getOrElse("output", "results/precomputedBF")) )

}

object PrecomputedBFRunner extends App {
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner
  val timer = io.scalding.approximations.Utils.withTimeCalc("PrecomputedBF time") {
    ToolRunner.run(new Configuration, new Tool, (classOf[PrecomputedBF].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}