name := "spark-mongo-iot"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.0.0",
  "net.liftweb" % "lift-webkit_2.10" % "2.6.3"
)
    