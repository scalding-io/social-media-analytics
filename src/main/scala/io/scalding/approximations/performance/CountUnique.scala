package io.scalding.approximations.performance

import com.twitter.algebird.HyperLogLogAggregator
import com.twitter.scalding._

/**
 * A Scalding job to count unique elements using HLL
 */
class CountUniqueHLL(args: Args) extends Job(args) {

  val input = args.getOrElse("input", "datasets/100MillionUnique")
  val output = args.getOrElse("output", "datasets/100MillionUniqueHLLCardinality")

  // HLL Error is about 1.04/sqrt(2^{bits}), so you want something like 12 bits for ~2% error
  // which means each HLLInstance is about 2^{12} = 4kb per instance.
  val unique = HyperLogLogAggregator
    .sizeAggregator(12)
    .composePrepare[String](_.getBytes("UTF-8"))

  val countUniquePipe = TypedPipe.from(TypedTsv[(String)](input))
    .aggregate(unique)
    .map { x => println(s"Cardinality of dataset: : $x"); x }
    .write(TypedTsv(output))

}

/**
 * A Scalding job to count unique elements using HLL
 */
class CountUnique(args: Args) extends Job(args) {

  val input = args.getOrElse("input", "datasets/100MillionUnique")
  val output = args.getOrElse("output", "datasets/100MillionUniqueCardinality")

  val countUniquePipe = TypedPipe.from(TypedTsv[(String)](input))
    .distinct
    .groupAll
    .sum
    //Throw away the unit key created by groupAll
    .values
    .map { x => println(s"Cardinality of dataset: : $x"); x }
    .write(TypedTsv(output))

}
