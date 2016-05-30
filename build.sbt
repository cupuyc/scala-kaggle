name := "scala-kaggle"

version := "1.0"


scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-math3" % "3.5",
  "org.jfree" % "jfreechart" % "1.0.17",
  "com.typesafe.akka" %% "akka-actor" % "2.3.4",
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-mllib" % "1.5.0",
  "org.scalatest" %% "scalatest" % "2.2.2"
)


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0"

libraryDependencies += "jline" % "jline" % "2.11"

libraryDependencies += "oro" % "oro" % "2.0.8"

// Resolver for Apache Spark framework
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

// Options for the Scala compiler should be customize
scalacOptions ++= Seq("-unchecked", "-optimize", "-language:postfixOps")

// Options for Scala test with timing and short stack trace
testOptions in Test += Tests.Argument("-oDS")

// Maximum number of errors during build
maxErrors := 30
    