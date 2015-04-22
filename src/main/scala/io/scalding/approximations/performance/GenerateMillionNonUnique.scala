package io.scalding.approximations.performance

import java.io._

/**
 * This will generate a single file with 90 Million unique strings of 10 bytes length / one string per line
 * plus another 10 Million strings (from a selection of 50 strings) resulting into a ~ 3 GB data-set !
 */
object GenerateMillionNonUnique extends App {

  // Removes control codes including private codes with regex - "[\\p{C}]"
  def generateNonUnique(filename:String, millions: Int, percentageUnique: Double) = {
    println(s"Generating '$filename' with $millions million ten character long strings - $percentageUnique % unique and ${1-percentageUnique} % repeated (one per line)")
    val lineSepartor = util.Properties.lineSeparator
    new File(filename).delete()
    val fw = new FileWriter(filename, true)

    // 90 Million unique
    for (i <- 1 to (percentageUnique * millions).toInt) {
      val chunk = (0 to 1000 * 1000).map(_ => util.Random.nextString(10).replaceAll("[\\p{C}]"," ")).mkString(lineSepartor)
      fw.write(chunk)
      println(s"$i million lines generated")
    }

    // 10 Million repeated
    for (i <- 1 to ((1-percentageUnique) * millions).toInt ) {
      val value = util.Random.nextInt(50)
      val chunk = (0 to 1000 * 1000).map(_ => (5000000000L - value)).mkString(lineSepartor)
      fw.write(chunk)
      println(s"$i million lines generated")
    }

    fw.close()
    println(s" File '$filename' generated ")
  }

  generateNonUnique("datasets/100MillionNonUnique",100,0.90) // 100 M - 90% of it unique
  generateNonUnique("datasets/10MillionNonUnique",  10,0.90) // 10  M - 90% of it unique
  generateNonUnique("datasets/1MillionNonUnique",    1,0.90) // 1   M - 90% of it unique

}