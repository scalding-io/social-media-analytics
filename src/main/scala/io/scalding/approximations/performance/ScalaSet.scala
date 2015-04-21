package io.scalding.approximations.performance

/**
 * A simple scala app - to demonstrate how much space on disk a mutable Set requires
 */
object ScalaSet extends App {

  val unique_visitor = collection.mutable.Set[String]()
  for (i <- 1 to 10000000) unique_visitor.add(util.Random.nextString(10))

  // 10 Million * 10 Bytes - on a mutable Set results into 326 MBytes of data
  scala.tools.nsc.io.File("10Million10Bytes").writeBytes(Utils.serialize(unique_visitor))
  println(s"Written ${unique_visitor.size} elements of 10 Bytes on file 10Million10Bytes")
}