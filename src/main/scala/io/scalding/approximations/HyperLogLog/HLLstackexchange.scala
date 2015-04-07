package io.scalding.approximations.HyperLogLog

import com.twitter.algebird._
import com.twitter.scalding._

/**
 * Counting the number of unique users in the stack exchange data-set using the Typed API
 *
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
object StackExchange {
  type StackExchangeType = (Long, Long, Long,Long, String, Long, Long, String,String)
  def fromTuple(t: StackExchangeType) = StackExchange(t._1, t._2, t._3, t._4, t._5,t._6,t._7,t._8,t._9)
}
case class StackExchange(ID: Long, PostTypeID: Long, ParentID: Long, OwnerUserID: Long, CreationDate: String,
                ViewCount: Long, FavoriteCount: Long, Tags: String, Keywords: String)

class HLLstackexchange(args: Args) extends Job(args) {

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
    .map { x => println(s"Cardinality estimation (unique users in stack exchange data) : $x"); x }
    .write(TypedTsv(output))

}
