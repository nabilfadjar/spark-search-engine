// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

// Project name
name := "TfIdfQueryEnigneCombined"

// Don't forget to set the version
version := "1.0.0-SNAPSHOT"

// scala version to be used
scalaVersion := "2.10.5"

// spark version to be used
val sparkVersion = "1.6.0"

// spark modules
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
)
