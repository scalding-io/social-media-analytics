package io.scalding.approximations.BloomFilter

import cascading.tuple.{Tuple, TupleEntry}
import com.twitter.scalding._
import com.twitter.algebird.{BFItem, BloomFilterMonoid, BF, BloomFilter}
import com.twitter.scalding.typed.TDsl._
import io.scalding.approximations.ExamplesRunner._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.util.ToolRunner
import java.net.URI

/**
 * We generate 100.000 user ids ( 1 .. 100000 ) and add them into a BloomFilter
 * with a small estimation error.
 * We save the Bloom Filter generated to reuse it in the following process
 *
 * The second job execute membership queries on some ids using the previously generated filter
 *
 * @author Antonios.Chalkiopoulos - http://scalding.io
 */
class BFSetGenerator(args:Args) extends Job(args) {

  case class User(userID: String)

  val size = 100000
  val fpProb = 0.01

  implicit val bloomFilterMonoid = BloomFilter(size, fpProb)

  // Generate and add 100K ids into the (Bloom) filter
  val usersList = (1 to size).toList.map{ x => User(x.toString) }
  val usersBF = IterableSource[User](usersList)
    .map { user => bloomFilterMonoid.create(user.userID) }
    .sum


  val serialiazedBfFile = args("serialized")
  println(s"Generating Bloom Filter and writing it into $serialiazedBfFile")

  // Serialize the BF
  usersBF
    .map { bf:BF => io.scalding.approximations.Utils.serialize(bf) }
    .toPipe('bloomFilter)
    .write( TextLine(serialiazedBfFile) )


  val bf = usersBF collect { case bf => bf }
}

class BFSetConsumer(args:Args) extends Job(args) {
  import com.twitter.scalding.filecache.DistributedCacheFile

  val serialiazedBfFile = DistributedCacheFile(args("serialized"))

  val usersBF = io.scalding.approximations.Utils.deserialize[BF]( serialiazedBfFile.file )

  println(s"Reading Bloom Filter and from $serialiazedBfFile")

  IterableSource( List( "ABCD", "EFGH", "123" ), 'toBeMatched )
    .read
    .map('toBeMatched -> 'matches) { toBeMatched: String =>
      val matches = if (usersBF.contains(toBeMatched).isTrue) "maybe" else "no"
      println( s"BF contains '$toBeMatched' ? $matches"  )
      matches
    }
    .write( Csv(args("output")) )
}

object BFSetGenerator {

  def run(bfFilePath: String, outputPath: String, args: Array[String], runMode: String = "--local" ) : Unit = {
    val config = new Configuration

    ToolRunner.run(config, new Tool, (classOf[BFSetGenerator].getName :: runMode ::
      "--serialized" :: bfFilePath ::  args.toList).toArray)

    DistributedCache.addCacheFile(new URI(bfFilePath), config)

    ToolRunner.run(config, new Tool, (classOf[BFSetConsumer].getName :: runMode ::
      "--output" :: outputPath :: "--serialized" :: bfFilePath ::  args.toList).toArray)
  }


  def main(args: Array[String]): Unit = run("results/BF-SimpleExample-serialized.tsv", "results/BF-SimpleExample.tsv", args, "--local")

}