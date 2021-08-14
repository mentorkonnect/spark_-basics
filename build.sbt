name := "SparkTutorial"

version := "0.1"

scalaVersion := "2.12.13"
val sparkVersion = "3.1.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
