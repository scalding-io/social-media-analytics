package io.scalding.approximations.performance

import java.io._

/**
 * This will generate a single file with 100 Million unique strings of 10 bytes length / one string per line
 * resulting into a ~ 3 GB data-set !
 */
object Generate100MillionUnique extends App {

  val filename = "datasets/100MillionUnique"
  new File(filename).delete()
  val fw = new FileWriter(filename, true)

  for (i<- 1 to 1000) {
    val chunk = (1 to 100*1000).map(_ => util.Random.nextString (10) ).mkString(util.Properties.lineSeparator )
    fw.write(chunk)
    if (i % 10 == 0) println(s"${i/10} % completed")
  }
  fw.close()
  println(s" File $filename generated ")

}

