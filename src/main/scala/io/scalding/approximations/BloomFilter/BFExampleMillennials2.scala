package io.scalding.approximations.BloomFilter

import com.twitter.algebird._
import com.twitter.scalding._

/**
 * @author Antonios Chalkiopoulos
 */
class BFExampleMillennials2(args:Args) extends Job(args) {

  case class User(userID: String, yearBorn: Int)
  val size = 100000
  val fpProb = 0.02D

  implicit val bloomFilterMonoid = BloomFilter(size, fpProb)              // Line 3

  val NUM_HASHES = 6
  val WIDTH = 32
  val SEED = 1
  val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
  val goodItems = List[String]("1", "2", "3", "4", "100")
  val bf = bfMonoid.create(goodItems:_*)
  val approxBool = bf.contains("1")
  val res = approxBool.isTrue
  val items = List("1", "2", "3", "4","10", "20", "30", "40", "100")
  val postBloom = items.filter(bf.contains(_).isTrue) // may have false positives

  def randomYear = 1991

  // 1st hour - the page got 100 K unique visitors
  val hour1List = (1 to size).toList
  val hour1 = IterableSource[Int](hour1List, 'data)
    .groupAll { group => group.mkString('data, ",") }
    .mapTo('data -> 'bloomfilter) {
      x:String => bfMonoid.create(x.split(','):_*)
    }
    .map('bloomfilter->'bloomfilter) {
    x: BF => x.contains("10000").isTrue + " " + x.contains("200001")
  }
    .debug
//  bfMonoid.create(group:_*)
//    .map('numbers -> ('userID , 'yearBorn)) {
//    x:Int => (x+"" , randomYear)
//  }
//    //.filter('yearBorn) { year:Int => (year > 1980 & year < 2000) }
//    .map('userID-> 'bf) { user:String => bloomFilterMonoid.create(user) }
//    .project('yearBorn,'bf)
//    .groupBy('yearBorn) { group => group.reduce[BF]('bf -> 'bf) {
//    (left:BF, right:BF) => bloomFilterMonoid.plus(left, right)
//  }
//  }
//    .map('bf->'bfw) { bfs: BFSparse =>
//      val existingItem : ApproximateBoolean = bfs.contains("10")
//      val nonExistingItem : ApproximateBoolean = bfs.contains("100000000000")
//
//      println(" Existing itam " + existingItem.ensuring(true, "hi"))
//      println(" -10 " + bfs.contains("10000000000").ensuring(false,"no"))
//      bfs
//    }
    .write(Tsv("results/BF-MillennialsExample.tsv"))
  //    .groupAll { group =>
  //      group.hyperLogLog[String](('numbers ->'denseHHL) , inaccuracy)
  //    }

  //  val userData = Csv(args("users"), fields = Users.fields, skipHeader = true).read
  //    .toTypedPipe[User.userTuple](User.fields) .map { User.fromTuple }  // Line 2


  //  val millenialsBloom = userData
  //    .filter { user => (user.yearBorn > 1980 & user.yearBorn < 2000) }  // Line 4
  //    .map { user => bloomFilterMonoid.create(user.id)) }                // Line 5
  //    .sum

}