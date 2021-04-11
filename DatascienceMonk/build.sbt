import org.apache.logging.log4j.core.config.composite.MergeStrategy
import sun.security.tools.PathList

name := "DatascienceMonk"

version := "0.1"

scalaVersion := "2.12.0"

name := "Spark1"

version := "0.1"

scalaVersion := "2.12.0"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.1"

resolvers ++= Seq(
  "Redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"
)

// Reading data from s3 SBT dependency
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.4"

// Avro dependency
libraryDependencies += "com.databricks" % "spark-avro_2.11" % "3.2.0"

// SFTP dependency
libraryDependencies += "com.springml" % "spark-sftp_2.11" % "1.1.1"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.15"

// AWS Redshift related dependency
libraryDependencies += "com.databricks" % "spark-redshift_2.11" % "3.0.0-preview1"
libraryDependencies += "com.amazon.redshift" % "redshift-jdbc42" % "1.2.1.1001"
dependencyOverrides += "com.databricks" % "spark-avro_2.11" % "3.2.0"

//configuration file dependency
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"

/*assemblyMergeStrategy in assembly := {

  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}*/



