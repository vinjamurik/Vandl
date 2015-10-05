name := "ElasticConnector"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.3.0" % "provided"
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.1" intransitive()

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "elaspark.jar"