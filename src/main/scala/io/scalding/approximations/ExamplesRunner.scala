package io.scalding.approximations

import com.twitter.scalding.Tool
import io.scalding.approximations.CMSketch._
import org.apache.hadoop.util.ToolRunner

/**
  */
object ExamplesRunner extends App {

  val configuration = new org.apache.hadoop.conf.Configuration

  println(s"Running Count-Min Sketch example, ")
  ToolRunner.run(configuration, new Tool,(classOf[CMSketch].getName ::
    (List("--local",
      "--input","datasets/stackexchange/posts.tsv",
      "--output","results/bloomFilter"))).toArray)

  //  println
  //  println(s"Combined SNAPSHOT file ${snapshotFile} with ${deltaFile} into ${outputFile}")

}