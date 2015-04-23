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

  val countUniquePipe = TypedPipe.from(TextLine(input))
    .aggregate(unique)
    .map { x => println(s"Cardinality of dataset: : $x"); x }
    .write(TypedCsv(output))

}

/**
 * A Scalding job for 100% accurate unique count
 */
class CountUnique(args: Args) extends Job(args) {

  val input = args.getOrElse("input", "datasets/10MillionUnique")
  val output = args.getOrElse("output", "datasets/10MillionUniqueCardinality")

  val countUniquePipe = TypedPipe.from(TextLine(input))
    .distinct
    .sum
    .map { x => println(s"Cardinality of dataset: : $x"); x }
    .write(TypedTsv(output))

}
