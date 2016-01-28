name := "vandl-spark"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0" % "provided"
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.11" % "2.2.0-m1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName := "vandl-spark.jar"
    