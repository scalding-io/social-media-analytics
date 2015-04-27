package io.scalding.approximations.HyperLogLog

import com.twitter.algebird._
import com.twitter.scalding._
import io.scalding.approximations.model.StackExchange

/**
 * Counting the number of unique users in the stack exchange data-set using the Typed API
 *
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
class StackexchangeHLL(args: Args) extends Job(args) {

  val input  = args.getOrElse("input" ,"datasets/stackexchange/posts.tsv")
  val output = args.getOrElse("output","results/HLL-stackexchange")

  // HLL Error is about 1.04/sqrt(2^{bits}), so you want something like 12 bits for ~2% error
  // which means each HLLInstance is about 2^{12} = 4kb per instance.
  val unique = HyperLogLogAggregator
    .sizeAggregator(12)
    //convert OwnerUserID to UTF-8 encoded bytes as HyperLogLog expects a byte array.
    .composePrepare[StackExchange](_.OwnerUserID.toString.getBytes("UTF-8"))

  val stackExchangePosts = TypedPipe.from(TypedTsv[StackExchange.StackExchangeType](input))
    .map { StackExchange.fromTuple }
    .aggregate(unique)
    .map { x => println(s"Unique authors (cardinality estimation in stack exchange data) : $x"); x }
    .write(TypedTsv(output))

}
