package io.scalding.approximations.performance

import java.io.{FileWriter, File}

/**
 * This will generate a single file with 90 Million unique strings of 10 bytes length / one string per line
 * plus another 10 Million strings (from a selection of 50 strings) resulting into a ~ 3 GB data-set !
 */
object Generate100MillionNonUnique extends App {

  val filename = "datasets/100MillionNonUnique"
  new File(filename).delete()
  val fw = new FileWriter(filename, true)

  // 90 Million uniques
  for (i<- 1 to 900) {
    val chunk = (1 to 100*1000).map(_ => util.Random.nextString (10) ).mkString(util.Properties.lineSeparator )
    fw.write(chunk)
    if (i % 10 == 0) println(s"${i/10} % completed")
  }

  // 10 Million repeated
  for (i<- 1 to 100) {
    val value = util.Random.nextInt(50)
    val chunk = (1 to 100*1000).map(_ => (5000000000L - value) ).mkString(util.Properties.lineSeparator )
    fw.write(chunk)
    if (i % 10 == 0) println(s"${(i+90)/10} % completed")
  }



  fw.close()
  println(s" File $filename generated ")

}