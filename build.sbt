name := "spark-dynamodb"

version := "1.0"

scalaVersion := "2.11.8"

mainClass in (Compile, packageBin) := None

libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.160"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
