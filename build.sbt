import AssemblyKeys._ // put this at the top of the file

name := "approximated-algorithms"

Common.settings


javacOptions in ThisBuild ++= Seq(
  "-source", "1.7",
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  "-target:jvm-1.7",
  "-encoding", "UTF-8"
)

resolvers ++= Common.resolvers 

libraryDependencies ++= Common.dependencies

lazy val root = project.in( file(".") )
			.settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
			.settings(assemblySettings: _*)
