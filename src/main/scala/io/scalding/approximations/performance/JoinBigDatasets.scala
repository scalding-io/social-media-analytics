package io.scalding.approximations.performance

import com.twitter.scalding._
import com.twitter.algebird._

/**
 * A Scalding to do a BloomJoin on two sparse 100 Million element datasets
 *
 */
class JoinBigDatasetsBF(args: Args) extends Job(args) {

  val inputA = args.getOrElse("inputA", "datasets/100MillionUnique")
  val inputB = args.getOrElse("inputB", "datasets/100MillionNonUnique")
  val output = args.getOrElse("output", "datasets/JoinResultBF")


}

class JoinBigDatasets (args: Args) extends Job(args) {

  val inputA = args.getOrElse("inputA", "datasets/100MillionUnique")
  val inputB = args.getOrElse("inputB", "datasets/100MillionNonUnique")
  val output = args.getOrElse("output", "datasets/JoinResult")

}

