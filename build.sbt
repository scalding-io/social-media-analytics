import sbtassembly.AssemblyPlugin.autoImport._

organization  := "io.scalding"

version       := "1.0"

name          := "Social-Media-Analytics"

scalaVersion  := "2.10.4"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers := Seq(
  "Maven central" at "http://repo1.maven.org/maven2/",
  "Conjars Repo" at "http://conjars.org/repo",
  "Clojars Repo" at "http://clojars.org/repo",
  "Twitter Repo" at "http://maven.twttr.com",
  "Cloudera Repo" at "http://repository.cloudera.com/content/repositories/releases/",
  "Cloudera Repo2" at "https://repository.cloudera.com/content/groups/cloudera-repos/",
  "Cloudera public" at "https://repository.cloudera.com/artifactory/public/",
  "Scalaz Repo"  at "http://dl.bintray.com/scalaz/releases",
  "Sonatype Snapshot" at "https://oss.sonatype.org/content/repositories/snapshots"
)

val hadoopCoreVersion = "2.5.0-mr1-cdh5.3.1"
val hadoopVersion = "2.5.0-cdh5.3.1"
val scaldingVersion = "0.13.1"
val sl4jVersion = "1.7.7"
val specs2Version = "2.4.13"
val scalaTestVersion = "2.2.2"

libraryDependencies ++= {
  Seq(
    ("com.twitter"           %% "scalding-core"         % scaldingVersion)    .exclude("com.esotericsoftware.minlog", "minlog").force(),
    ("com.twitter"           %% "scalding-commons"      % scaldingVersion)    .exclude("org.mortbay.jetty","jetty").exclude("org.mortbay.jetty","jsp-api-2.1").exclude("org.mortbay.jetty","servlet-api-2.5").exclude("commons-beanutils","commons-beanutils").exclude("commons-beanutils","commons-beanutils-core"),
    ("org.apache.hadoop"      % "hadoop-common"         % hadoopVersion)      .excludeAll(ExclusionRule(organization="org.mortbay.jetty"),ExclusionRule(organization="com.sun.jersey"), ExclusionRule("commons-beanutils"), ExclusionRule("commons-configuration"), ExclusionRule("org.slf4j") ).exclude("com.sun.jersey","jersey-server"),
    "org.apache.hadoop"       % "hadoop-core"           % hadoopCoreVersion   excludeAll ExclusionRule("org.slf4j"),
    "com.twitter"             % "algebird-core_2.10"    % "0.9.0",
    "commons-configuration"   % "commons-configuration" % "1.10",
    "org.slf4j"               % "slf4j-api"             % sl4jVersion,
    "org.slf4j"               % "slf4j-log4j12"         % sl4jVersion,
    "org.specs2"             %% "specs2"                % specs2Version      % "test",
    "org.scalatest"          %% "scalatest"             % scalaTestVersion   % "test",
    "org.scala-lang.modules" %% "scala-pickling" % "0.10.0"
  )
}

// THE 'TOOL' ASSEMBLY IS DEPLOYABLE
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".sf" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".dsa" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".rsa" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last contains "jersey" => MergeStrategy.concat
  case PathList(ps @ _*) if ps.contains("manifest.mf") => MergeStrategy.concat
  case PathList(ps @ _*) if ps.last contains "application.conf" => MergeStrategy.concat
  case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "pom.xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "plugin.xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.contains("jackson") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  val excludesJar = Set(
    "jersey-core-1.9.jar",
    "protobuf-java-2.5.0.jar",
    "commons-lang-2.6.jar",
    "libthrift-0.9.0-cdh5-2.jar",
    "jackson-core-asl-1.9.13.jar",
    "jackson-mapper-asl-1.9.13.jar",
    "junit-4.8.1.jar",
    "jersey-servlet-1.14.jar",
    "stax-api-1.0.1.jar"
  )
  cp filter { jar => excludesJar.contains(jar.data.getName)}
}

net.virtualvoid.sbt.graph.Plugin.graphSettings