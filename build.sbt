import sbtassembly.AssemblyKeys

name := "scala-kaggle"

version := "0.1"


scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint")


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-hive" % "1.6.1",
  "com.databricks" %% "spark-csv" % "1.2.0"


//  "org.apache.commons" % "commons-math3" % "3.5"
//  "com.typesafe.akka" %% "akka-actor" % "2.3.4",
//  "org.scalatest" %% "scalatest" % "2.2.2"
)

mainClass in (Compile, run) := Some("io.github.stanreshetnyk.expedia.ExpediaSpark")

mainClass in (Compile, packageBin) := Some("io.github.stanreshetnyk.expedia.ExpediaSpark")

//libraryDependencies += "jline" % "jline" % "2.11"

//libraryDependencies += "oro" % "oro" % "2.0.8"

// Resolver for Apache Spark framework
//resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

// Options for the Scala compiler should be customize
//scalacOptions ++= Seq("-unchecked", "-optimize", "-language:postfixOps")

// Options for Scala test with timing and short stack trace
//testOptions in Test += Tests.Argument("-oDS")

// Maximum number of errors during build
//maxErrors := 30

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
  case "about.html"  => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

fork in run := true