package io.scalding.approximations.BloomFilter

import com.twitter.scalding.Tool
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration

object BloomFilterExample extends App {

  val historical = "./datasets/SFPD_Incidents_Previous_Three_Months_Inflated.csv"
  val daily = "./datasets/SFPD_Incidents_Previous_Day.csv"
  val output = "bloomFilterSPFDAnalysis.csv"
  val noFilterOutput = "noBloomFilterSPFDAnalysis.csv"

  println( s"Analysing daily incident file '$daily' matching with historical incidents at '$historical' with and without bloom filter " +
    s"and writing output with bloom filter at '$output' and without at $noFilterOutput" )

  val withFilter = withTimeCalc("Using Bloom Filter") {

    val jobArgs =
      classOf[ExtractSimilarHistoryForDailyIncidentsWithBloomFilter].getName :: "--hdfs" ::
        "--historical" :: historical ::
        "--daily" :: daily ::
        "--output" :: output ::
        args.toList

    hadoop.util.ToolRunner.run(new Configuration, new Tool, jobArgs.toArray)
  }

  val withoutFilter =withTimeCalc("NOT Using Bloom Filter") {
    val jobArgs =
      classOf[ExtractSimilarHistoryForDailyIncidentsWithNoBloomFilter].getName :: "--hdfs" ::
        "--historical" :: historical ::
        "--daily" :: daily ::
        "--output" :: noFilterOutput ::
        args.toList

    hadoop.util.ToolRunner.run(new Configuration, new Tool, jobArgs.toArray)
  }

  println( s"With bloom filter took $withFilter millis, without took $withoutFilter millis")

  
  def withTimeCalc(blockDescr: String)( block : => Unit): Long = {
    val start = System.currentTimeMillis()
    var end = 0l

    try {
      block
    } finally {
      end = System.currentTimeMillis()
    }

    end - start
  }
}
