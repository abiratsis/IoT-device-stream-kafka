name := "IoT-device-stream-kafka"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  val hbaseVer = "1.3.1"
  val kafkaVer = "0.11.0.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" % "spark-streaming_2.11" % sparkVer,
    "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVer,

    "org.json4s" %% "json4s-jackson" % "3.2.11",
    "org.json4s" %% "json4s-native" % "3.2.11",

    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,

    "org.apache.kafka" %% "kafka" % kafkaVer,
    "org.apache.kafka" % "kafka-clients" % kafkaVer,

    "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
    "org.apache.logging.log4j" % "log4j-core" % "2.11.0" % Runtime,
    "org.scala-lang" % "scala-swing" % "2.11+",

    "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test",
    "net.manub" %% "scalatest-embedded-kafka" % "1.1.0" % Test,

    "org.apache.hbase" % "hbase-server" % hbaseVer,
    "org.apache.hbase" % "hbase-client" % hbaseVer,
    "org.apache.hbase" % "hbase-common" % hbaseVer,

    "org.scalacheck" %% "scalacheck" % "1.14.0"
  )
}

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5"