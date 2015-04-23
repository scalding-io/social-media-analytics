package io.scalding.approximations.performance

import com.twitter.scalding._
import com.twitter.algebird._

/**
 * A Scalding application performing a BloomJoin on two datasets
 * BF is generated on input A - so A should be the smaller dataset
 */
class JoinBigDatasetsBF(args: Args) extends Job(args) {

  val inputA = args.getOrElse("inputA", "datasets/1MillionUnique")
  val inputB = args.getOrElse("inputB", "datasets/10MillionUnique")
  val bfEntries = args.getOrElse("bfsize","1000000").toInt
  val output = args.getOrElse("output", "results/JoinResultBF")

  // BF aggregator
  val bfAggregator = BloomFilterAggregator
    .apply(BloomFilter(numEntries = bfEntries , fpProb = 0.02))
    //.composePrepare[(String)](_)

  val pipeABF= TypedPipe.from(TextLine(inputA))
    .aggregate(bfAggregator)

  val pipeB= TypedPipe.from(TextLine(inputB))
    .filterWithValue(pipeABF) { (key, bfOption) =>
      bfOption map { _.contains( key.toString ).isTrue } getOrElse false
    }
    .map{ x => (x,"") }
    .write(TypedTsv(output))

}

/**
 * A Scalding application performing a join on two datasets
 */
class JoinBigDatasets (args: Args) extends Job(args) {

  val inputA = args.getOrElse("inputA", "datasets/1MillionUnique")
  val inputB = args.getOrElse("inputB", "datasets/1MillionUnique")
  val output = args.getOrElse("output", "results/JoinResult")

  val pipeA= TypedPipe.from(TextLine(inputA))
    .map { text => (text, "") } // Convert into (Key,Value) tuple

  val pipeB= TypedPipe.from(TextLine(inputB))
    .map { text => (text, "") } // Convert into (Key,Value) tuple

  pipeA.join(pipeB).keys
    .write(TypedTsv(output))

}

