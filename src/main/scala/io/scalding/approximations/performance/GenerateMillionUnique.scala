package io.scalding.approximations.performance

import java.io._

/**
 * This will generate a single file with 100 Million unique strings of 10 bytes length / one string per line
 * resulting into a ~ 3 GB data-set !
 */
object GenerateMillionUnique extends App {

  def generateUnique(filename:String, millions: Int) = {
    println(s"Generating '$filename' with $millions million unique ten character long strings, one per line")
    val lineSepartor = util.Properties.lineSeparator
    new File(filename).delete()
    val fw = new FileWriter(filename, true)

    for (i <- 1 to millions) {
      val chunk = (0 to 1000 * 1000).map(_ => util.Random.nextString(10).replaceAll("[\\p{C}]"," ")).mkString(lineSepartor)
      fw.write(chunk)
      println(s"$i million lines generated")
    }
    fw.close()
    println(s" File '$filename' generated ")
  }

  generateUnique("datasets/100MillionUnique",100)
  generateUnique("datasets/20MillionUnique",20)
  generateUnique("datasets/10MillionUnique",10)
  generateUnique("datasets/1MillionUnique",1)

}