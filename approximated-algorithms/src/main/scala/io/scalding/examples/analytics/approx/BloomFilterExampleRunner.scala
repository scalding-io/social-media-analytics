package io.scalding.examples.analytics.approx

import com.twitter.scalding.{Args, Tool}
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration

object BloomFilterExampleRunner extends App {

  val historical = "./datasets/SFPD_Incidents_Previous_Three_Months.csv"
  val daily = "./datasets/SFPD_Incidents_Previous_Day.csv"
  val output = "bloomFilterSPFDAnalysis.csv"

  println( s"Analysing daily incident file '$daily' matching with historical incidents at '$historical' and writing the output at '$output'" )

  val jobArgs =
    classOf[BloomFilterExample].getName :: "--local" ::
      "--historical" ::  historical ::
      "--daily" :: daily ::
      "--output" :: output ::
      args.toList

  hadoop.util.ToolRunner.run(new Configuration, new Tool, jobArgs.toArray );
}
