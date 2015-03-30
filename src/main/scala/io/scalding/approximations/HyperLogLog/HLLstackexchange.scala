package io.scalding.approximations.HyperLogLog

import cascading.pipe.Pipe
import com.twitter.algebird.DenseHLL
import com.twitter.scalding._

/**
 * Counting the number of unique users in the stack exchange data-set
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
class HLLstackexchange(args: Args) extends Job(args) {

  // The input schema of our data-set
  val schema = List('ID, 'PostTypeID, 'ParentID, 'OwnerUserID, 'CreationDate, 'ViewCount, 'FavoriteCount, 'Tags, 'Keywords)

  // Take in as arguments the location of the input/output files
  val inputFiles = args("input")
  val outputFile = args("output")

   val inaccuracy = 2D

   // Implicit conversion of text to bytes
   implicit def long2Bytes(num:Long): Array[Byte] = num.toString.getBytes

   // Helper method to print cardinality estimations on screen
   def printSizeOfHLL(pipe: Pipe, symbol: Symbol, name:String ) =
      pipe.mapTo( symbol -> symbol ) {
        hll: DenseHLL =>
          val estimation = hll.approximateSize.estimate
          println(s"$name cardinality estimation: $estimation with $inaccuracy % error")
          hll
      }

  val stackExchangePosts = Tsv(inputFiles,schema).read
    .project('OwnerUserID)
    .groupAll { group =>
       group.hyperLogLog[Long]('OwnerUserID ->'denseHHL , inaccuracy)
    }

   printSizeOfHLL(stackExchangePosts, 'denseHHL, "stackexchange")
     .write(TextLine(outputFile))

 }
