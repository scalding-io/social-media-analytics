package io.scalding.approximations.HyperLogLog

import com.twitter.algebird._
import com.twitter.scalding._

/**
 * Example of adding 2 HLL of ~ 100.000 elements each
 * resulting into a ~ 120.000 estimation as 80.000 elements exist in both sets
 *
 * This example uses the Scalding Typed API
 *
 * https://github.com/twitter/scalding/wiki/Aggregation-using-Algebird-Aggregators
 *
 * @author Antonios Chalkiopoulos - http://scalding.io
 */

class HLLExampleForTwoHours(args: Args) extends Job(args) {

  case class Users(userID: Int)

  // HLL Error is about 1.04/sqrt(2^{bits}), so you want something like 12 bits for ~2% error
  // which means each HLLInstance is about 2^{12} = 4kb per instance.
  val unique = HyperLogLogAggregator
    .sizeAggregator(12)
    //convert user names to UTF-8 encoded bytes as HyperLogLog expects a byte array.
    .composePrepare[Users](_.userID.toString.getBytes("UTF-8"))

  // 1st hour - a web page got 100 K unique visitors
  val hour1List = (1 to 100000).map( Users ).toList
  val hour1 = TypedPipe.from(hour1List)

  hour1
    .aggregate(unique)
    .map{ x => println(s"Cardinality of HOUR 1: $x"); x }
    .write(TypedTsv("results/HLL-1stHour"))

  // 2st hour - the page got 100 K unique visitors. 80 K ( 20,001 .. 100,000) of them were visitors in the previous hour as well
  val hour2List = (20001 to 120000).map( Users ).toList
  val hour2 = TypedPipe.from(hour2List)

  val hll = hour2
    .aggregate(unique)


  hour2
    .aggregate(unique)
    .map{ x => println(s"Cardinality of HOUR 2: $x"); x }
    .write(TypedTsv("results/HLL-2ndHour"))

  val unionTwoHours = (hour1 ++ hour2)
    .aggregate(unique)
    .map{ x => println(s"Cardinality of HOUR 1 & HOUR: 2 $x"); x }
    .write(TypedTsv("results/HLL-BothHours"))

}

object HLLExampleForTwoHoursRunner extends App {
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner
  val timer = io.scalding.approximations.Utils.withTimeCalc("Running HLLExampleForTwoHours in --local mode") {
    ToolRunner.run(new Configuration, new Tool, (classOf[HLLExampleForTwoHours].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}
