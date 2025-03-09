ThisBuild / version := "0.3.0"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "smart-farm-iot-monitoring-v4"
  )

val VersionSpark = "3.5.5"
val VersionCatsCore = "2.13.0"

// Importaciones necesarias para trabajar con Spark y Kafka
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.3",
  // Delta Lake
  "io.delta" %% "delta-spark" % "3.3.0",
)

val sparkDependencies: Seq[ModuleID] = Seq(
  "org.apache.spark" %% "spark-sql" % VersionSpark % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % VersionSpark % Provided,
  // Add Hive support
  "org.apache.spark" %% "spark-hive" % VersionSpark % Provided,
)


val testDependencies: Seq[ModuleID] = Seq(
  // ScalaTest for testing
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,

  // Mockito integration for ScalaTest
  "org.scalatestplus" %% "mockito-4-11" % "3.2.18.0" % Test,
  // Spark Fast Tests
  "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % Test
)

val catsDependencies: Seq[ModuleID] = Seq(
  "org.typelevel" %% "cats-core" % VersionCatsCore
)

libraryDependencies ++= catsDependencies ++ testDependencies ++ sparkDependencies