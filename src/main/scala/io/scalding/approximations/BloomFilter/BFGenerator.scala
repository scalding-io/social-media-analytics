package io.scalding.approximations.BloomFilter

import com.twitter.scalding._
import com.twitter.algebird.{BF, BloomFilter}
import com.twitter.scalding.typed.TDsl._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.util.ToolRunner
import java.net.URI

/**
 * Generate user ids ( 1 .. 100000 ) and add them into a BloomFilter with 1% error rate.
 * Store the BF and reuse it in the following process
 * @author Antonios Chalkiopoulos - http://scalding.io
 */
object RunBFExample {

  def run(bfFilePath: String, outputPath: String, args: Array[String], runMode: String = "--local" ) : Unit = {
    val config = new Configuration

    ToolRunner.run(config, new Tool, (classOf[BFGenerator].getName :: runMode ::
      "--serialized" :: bfFilePath ::  args.toList).toArray)

    DistributedCache.addCacheFile(new URI(bfFilePath), config)

    ToolRunner.run(config, new Tool, (classOf[BFConsumer].getName :: runMode ::
      "--output" :: outputPath :: "--serialized" :: bfFilePath ::  args.toList).toArray)
  }

  def main(args: Array[String]): Unit = run("results/BFSetGenerator.bf", "results/BF-SimpleExample.tsv", args, "--local")

}

case class SimpleUser(userID: String)

// Generate mock data and store BF
class BFGenerator(args:Args) extends Job(args) {

  val serialiazedBfFile = args.getOrElse("serialized","results/BFSetGenerator.bf")

  val size = 100000
  val fpProb = 0.01

  implicit val bloomFilterMonoid = BloomFilter(size, fpProb)

  // Generate and add 100K ids into the (Bloom) filter
  val usersList = (1 to size).toList.map{ x => SimpleUser(x.toString) }
  val usersBF = IterableSource[SimpleUser](usersList)
    .map { user:SimpleUser => bloomFilterMonoid.create(user.userID) }
    .sum
    .map { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
    .toPipe('bloomFilter)
    .write( SequenceFile(serialiazedBfFile) )

  println(s"Bloom Filter serialized at: $serialiazedBfFile")

}

// Read and use BF
class BFConsumer(args:Args) extends Job(args) {
  import com.twitter.scalding.filecache.DistributedCacheFile

  val usersBF = readCachedBloomFilter(args.getOrElse("serialized","results/BFSetGenerator.bf"))
  
  IterableSource( List( "ABCD", "EFGH", "123" ), 'toBeMatched )
    .read
    .toTypedPipe[String]('toBeMatched)
    .mapWithValue(usersBF) { (toBeMatched: String, usersFilterOp: Option[BF] )=>
      val matches = if (usersFilterOp.get.contains(toBeMatched).isTrue) "maybe" else "no"
      println( s"BF contains '$toBeMatched' ? $matches"  )
      (toBeMatched, matches)
    }
    .toPipe( 'toBeMatched, 'matches )
    .write( Csv(args("output")) )
  
  def readCachedBloomFilter(filePath: String): ValuePipe[BF] = {
    val size = 100000
    val fpProb = 0.01

    implicit val bloomFilterMonoid = BloomFilter(size, fpProb)
    
    val serialiazedBfFile = DistributedCacheFile(filePath)
    println(s"Reading Bloom Filter and from $serialiazedBfFile")

    SequenceFile(serialiazedBfFile.path, 'value )
      .read
      .mapTo('value -> 'bloomFilter) { serialized: Array[Byte] => io.scalding.approximations.Utils.deserialize[BF](serialized) }
      .toTypedPipe[BF]('bloomFilter)
      .sum
  }
}

