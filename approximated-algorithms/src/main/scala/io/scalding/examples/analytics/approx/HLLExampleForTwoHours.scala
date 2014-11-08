package io.scalding.examples.analytics.approx

import cascading.pipe.Pipe
import com.twitter.algebird.{HLL, DenseHLL}
import com.twitter.scalding._

/**
 * Example of adding 2 HLL of ~ 100.000 elements each
 * resulting into a ~ 120.000 estimation as 80.000 elements exist in both sets
 *
 * @since 08/11/2014 by Antwnis _at_ gmail
 */
class HLLExampleForTwoHours(args: Args) extends Job(args) {

  // Setting up example : 100K elements on each set, 2% inaccuracy using HLL (4 KBytes of memory)
  val setSize = 100000
  val inaccuracy = 2D

  // Implicit conversion of test to bytes
  implicit def text2Bytes(text:String) = text.getBytes

  // Helper method to print cardinality estimations on screen
  def printSizeOfHLL(pipe: Pipe, symbol: Symbol, name:String ) =
     pipe.mapTo( symbol -> symbol ) {
       hll: DenseHLL =>
         val estimation = hll.approximateSize.estimate
         println(s"Cardinality estimation of (${name}) set : ${estimation} with 4% estimation error")
         hll
     }

  // 1st hour - the page got 100 K unique visitors
  val hour1List = (1 to setSize).toList
  val hour1 = IterableSource[Int](hour1List, 'numbers)
    .groupAll { group =>
      group.hyperLogLog[String](('numbers ->'denseHHL) , inaccuracy)
    }

  // 2st hour - the page got 100 K unique visitors. 80 K of them were visitors in the previous hour as well
  // and there are 20 K more new unique users this hour
  val hour2List = (20000 to setSize+20000).toList
  val hour2 = IterableSource[Int](hour2List, 'numbers)
    .groupAll { group =>
      group.hyperLogLog[String](('numbers ->'denseHHL) , inaccuracy)
    }

  printSizeOfHLL(hour1, 'denseHHL, "1st hour")
    .write(TextLine("HLLof1stHour"))

  printSizeOfHLL(hour2, 'denseHHL, "2nd hour")
    .write(TextLine("HLLof2ndHour"))

  val unionTwoHours = (hour1 ++ hour2)
    .groupAll { group =>
      group.reduce('denseHHL -> 'denseHHL) {
        (left:HLL,right:HLL) => left + right
      }
    }

  printSizeOfHLL(unionTwoHours, 'denseHHL, "1st and 2nd hour")
    .write(TextLine("HLLof1stand2ndHour"))

}
