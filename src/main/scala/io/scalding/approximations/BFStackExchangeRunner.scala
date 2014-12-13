package io.scalding.approximations

import com.twitter.scalding.Tool
import io.scalding.approximations.BloomFilter.BFStackExchangeUsersPostExtractor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner

/**
 * This is effictively equivelant to
 *  $ java -jar
 */
object BFStackExchangeRunner extends App {

  ToolRunner.run(new Configuration, new Tool, (classOf[BFStackExchangeUsersPostExtractor].getName ::
    "--local" :: "--minAge" :: "18" :: "--maxAge" :: "21" ::
    "--posts" :: "datasets/stackexchange/posts.tsv" :: "--users" :: "datasets/stackexchange/Users.xml" ::
    "--output":: "results/BF-StackExchangeUsersPost.tsv" :: args.toList).toArray)

}
