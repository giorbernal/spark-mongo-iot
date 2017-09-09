name := "spark-mongo-iot"

version := "1.2"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" % "spark-hive_2.11" % "2.1.0" % "provided",
  "org.apache.spark" % "spark-hive-thriftserver_2.11" % "2.1.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.0.0",
  "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.4.0",
  "au.com.bytecode" % "opencsv" % "2.4",
  "net.liftweb" % "lift-webkit_2.10" % "2.6.3"
)

mainClass in assembly := some("es.bernal.sparkmongoiot.Analytic")
assemblyJarName := "analytics-1.2.jar"

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}
