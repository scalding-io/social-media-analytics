package io.scalding.approximations

import com.twitter.algebird._

/**
 * To calculate the size of CMS / HLL / BF for particular sets while serializing into files.
 */
object Estimator extends App {

  implicit val bfMonoid1percent = com.twitter.algebird.BloomFilter(1000000, 0.01D)    // Our Bloom Filter monoid
  val alphanumeric = scala.util.Random.alphanumeric
  val termFrequencies = bfMonoid1percent.create(Stream.range(1 , 1000000).map( _ => alphanumeric.take(10).mkString):_*)
  val bf1pcSerialized = termFrequencies.toString
  scala.tools.nsc.io.File("filename1").writeAll(bf1pcSerialized)


  val hll = new HyperLogLogMonoid(10000000)
  val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
  val seqHll = Stream.range(1 , 1000000).map { _ => hll(alphanumeric.take(10).mkString.getBytes("utf-8")) }
//  val sumHll = hll.sum(seqHll)

//  scala.tools.nsc.io.File("filename2").writeAll(sumHll.toDenseHLL)

  println("done")
  //  val bf1pc: BF = Stream.range(1 , 10000000)
  //    .foldLeft(bfMonoid1percent.zero)( (bf:BF, x:Int) => {
  //      if (x%100000 ==0) println(x)
  //      bf.+(alphanumeric.take(10).mkString) }
  //    )
  //io.scalding.approximations.Utils.serialize(



  //  val usersList = (1 to size).toList
//  val usersBF = IterableSource[Int](usersList,'id)
//    .groupAll { group =>
//      group.foldLeft('id -> 'bloom)(bfMonoid1percent.zero) {
//        (bf: BF, id: Int) => {
//            if (id % 100 == 0) println(id)
//            bf + (id + "")
//          }
//        }
//      }
//
//  usersBF.write(Tsv("peiler"))
//  usersBF.write(SequenceFile("peiler2"))
//
//  usersBF
//  .mapTo('bloom ->'serialized) { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
//  .write(Tsv("peiler3"))

}
