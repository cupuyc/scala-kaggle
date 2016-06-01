name := "scala-kaggle"

version := "0.1"


scalaVersion := "2.10.6"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.1",
  "com.databricks" %% "spark-csv" % "1.2.0",


  "org.apache.commons" % "commons-math3" % "3.5",
  "org.jfree" % "jfreechart" % "1.0.17",
  "com.typesafe.akka" %% "akka-actor" % "2.3.4",
  "org.scalatest" %% "scalatest" % "2.2.2"
)


//libraryDependencies += "jline" % "jline" % "2.11"

libraryDependencies += "oro" % "oro" % "2.0.8"

// Resolver for Apache Spark framework
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

// Options for the Scala compiler should be customize
scalacOptions ++= Seq("-unchecked", "-optimize", "-language:postfixOps")

// Options for Scala test with timing and short stack trace
testOptions in Test += Tests.Argument("-oDS")

// Maximum number of errors during build
maxErrors := 30
    