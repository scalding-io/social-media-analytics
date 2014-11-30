package io.scalding.approximations.HyperLogLog

import cascading.pipe.Pipe
import com.twitter.algebird.DenseHLL
import com.twitter.scalding._

/**
 * Counting the number of unique contributors in the wikipedia data-set
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
class HLLwikipedia(args: Args) extends Job(args) {

  // The input schema of our data-set
  val schema = List('ContributorID, 'ContributorUserName, 'RevisionID, 'DateTime)

  // Take in as arguments the location of the input/output files
  val inputFiles = args("input")
  val outputFile = args("output")

   val inaccuracy = 2D

   // Implicit conversion of text to bytes
   implicit def long2Bytes(num:Long) = num.toString.getBytes

   // Helper method to print cardinality estimations on screen
   def printSizeOfHLL(pipe: Pipe, symbol: Symbol, name:String ) =
      pipe.mapTo( symbol -> symbol ) {
        hll: DenseHLL =>
          val estimation = hll.approximateSize.estimate
          println(s"Cardinality estimation of (${name}) set : ${estimation} with ${inaccuracy} % estimation error")
          hll
      }

  val wikipediaPosts = Tsv(inputFiles,schema).read
    .project('ContributorID)
    
    .groupAll { group =>
       group.hyperLogLog[Long](('ContributorID ->'denseHHL) , inaccuracy)
    }

   printSizeOfHLL(wikipediaPosts, 'denseHHL, "wikipedia")
     .write(TextLine(outputFile))

 }
