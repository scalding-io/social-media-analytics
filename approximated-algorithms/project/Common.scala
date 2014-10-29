import sbt._
import Keys._

object Common {

  val settings: Seq[Setting[_]] = Seq (
    version := "1.0",
    scalaVersion := "2.10.4",
    organization := "scalding.io"
  )

  val conjarResolver = "conjars.org" at "http://conjars.org/repo"
  val twitterResolver = "twitter" at "http://maven.twttr.com"

  val resolvers = Seq(
      conjarResolver,
      twitterResolver
    )

  val jodaTime = Seq( "joda-time" % "joda-time" % "2.0", "org.joda" % "joda-convert" % "1.2" )

  val walletCommon = "parallelai.wallet" %% "common" % "1.0" changing()


  val scaldingVersion = "0.11.0"
  
  val scaldingCore = "com.twitter" %% "scalding-core" % scaldingVersion
  val scaldingCommons = "com.twitter" %% "scalding-commons" % scaldingVersion
  val scaldingDate = "com.twitter" %% "scalding-date" % scaldingVersion
  val twitterUtil = "com.twitter" %% "util" % "6.22.1"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.0"

  val dependencies = Seq(
      scaldingCore,
      scaldingCommons,
      scaldingDate,
      twitterUtil,
      scalaTest % "test"
    )
}