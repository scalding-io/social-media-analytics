package io.scalding.examples.analytics.approx

import com.twitter.scalding.{Args, Tool}
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration

object BloomFilterExampleRunner extends App {

  val jobArgs =
    classOf[BloomFilterExample].getName :: "--local" ::
      "--historical" :: "./datasets/SFPD_Incidents_Previous_Three_Months.csv" ::
      "--daily" :: "./datasets/SFPD_Incidents_Previous_Day.csv" ::
      "--output" :: "bloomFilterSPFDAnalysis.csv" ::
      args.toList

  hadoop.util.ToolRunner.run(new Configuration, new Tool, jobArgs.toArray );
}
