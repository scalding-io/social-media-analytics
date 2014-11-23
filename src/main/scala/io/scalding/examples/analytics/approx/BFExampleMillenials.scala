package io.scalding.examples.analytics.approx

import cascading.tuple.Fields
import com.twitter.algebird.{BF, BloomFilterMonoid, BloomFilter}
import com.twitter.scalding._

/**
 * Created by Antwnis on 09/11/2014.
 */
class BFExampleMillenials(args:Args) extends Job(args) {

  case class User(userID: String, yearBorn: Int)
  val size = 100000
  val fpProb = 2D

  implicit val bloomFilterMonoid = BloomFilter(size, fpProb)              // Line 3

  def randomYear = 1991

  val groupByList = "a,b,c"
  val groupBySymbols = groupByList.split(",").map ( x => Symbol(x) ).toList // This is a List[Symbol]
  val groupByFields : Fields = new Fields( "a","b","c") //groupByList.split(","):_* )

  // 1st hour - the page got 100 K unique visitors
  val hour1List = (1 to size).toList
  val hour1 = IterableSource[Int](hour1List, 'numbers)
    .map('numbers -> ('userID , 'yearBorn)) {
       x:Int => (x+"" , randomYear)
    }
    .filter('yearBorn) { year:Int => (year > 1980 & year < 2000) }
    .map('userID-> 'bf) { user:String => bloomFilterMonoid.create(user) }
    .project('yearBorn,'bf)
    .groupBy('yearBorn) { group => group.reduce[BF]('bf -> 'bf) {
       (left:BF, right:BF) => bloomFilterMonoid.plus(left, right)
      }
    }
    .write(Tsv("target/BFExampleMillenials.tsv"))
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
