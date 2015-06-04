import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

seq(assemblySettings: _*)

name := "RDD_Filter"

version := "0.1"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

exportJars :=true

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

val meta = """META.INF(.)*""".r 

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => { case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last case PathList("javax", "activation", xs @ _*) => MergeStrategy.last case PathList("org", "apache", xs @ _*) => MergeStrategy.last case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last case PathList("plugin.properties") => MergeStrategy.last case meta(_) => MergeStrategy.discard case x => old(x) } }
