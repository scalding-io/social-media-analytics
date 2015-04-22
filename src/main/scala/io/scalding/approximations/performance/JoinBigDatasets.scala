package io.scalding.approximations.performance

import com.twitter.scalding._
import com.twitter.algebird._

/**
 * A Scalding application performing a BloomJoin on two datasets
 *
 */
class JoinBigDatasetsBF(args: Args) extends Job(args) {

  val inputA = args.getOrElse("inputA", "datasets/1MillionUnique")
  val inputB = args.getOrElse("inputB", "datasets/1MillionUnique")
  val output = args.getOrElse("output", "results/JoinResultBF")

  // BF aggregator
  val bfAggregator = BloomFilterAggregator
    .apply(BloomFilter(numEntries = 1000*1000 , fpProb = 0.02))
//    .composePrepare[(String)](_)


  val pipeABF= TypedPipe.from(TextLine(inputA))
    .aggregate(bfAggregator)

  val pipeB= TypedPipe.from(TextLine(inputA))
//    .filterWithValue(pipeABF) { (key, bfOption) =>
//      bfOption map { _.contains( key.toString ).isTrue } getOrElse false
//    }
    .write(TypedTsv("peilerA"))

}

class JoinBigDatasets (args: Args) extends Job(args) {

  val inputA = args.getOrElse("inputA", "datasets/1MillionUnique")
  val inputB = args.getOrElse("inputB", "datasets/1MillionUnique")
  val output = args.getOrElse("output", "results/JoinResult")

  val pipeA= TypedPipe.from(TextLine(inputA))
    .map { text => (text, "") } // Convert into (Key,Value) tuple

  val pipeB= TypedPipe.from(TextLine(inputB))
    .map { text => (text, "") } // Convert into (Key,Value) tuple

  pipeA.join(pipeB).keys
    .write(TypedTsv("peiler"))

}

