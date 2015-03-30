package io.scalding.approximations.BloomFilter

import com.twitter.scalding._

import scala.util.Random

/**
 * A simple data generator to simulate the a set of users for the `Millennials` example
 *
 * @author Antonios Chalkiopoulos - http://scalding.io
 */
class DataGenerator(args: Args) extends Job(args) {

  val size = 200000
  val usersList = (1 to size).toList
  val mockData = IterableSource[Int](usersList, 'userid)
    .map('userid -> 'yearBorn) {
      // The first 100 K userid will be born before 1980
      x: Int =>
        val randomness = (new Random).nextInt(20)
        if (x < 100000) 1979-randomness else 1980+randomness
    }
    .write(Tsv("datasets/millennials/userdata.tsv"))

}
