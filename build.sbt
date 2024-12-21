scalaVersion := "2.12.20"

name := "sparksql-parser"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
