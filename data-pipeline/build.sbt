name := "data-pipeline"
version := "0.1.0"
scalaVersion := "2.12.18"

// Dependencias core
libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % "3.5.7" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.7" % "provided",

  // DuckDB
  "org.duckdb" % "duckdb_jdbc" % "0.9.2",

  // Delta Lake
  "io.delta" %% "delta-spark" % "3.2.1",

  // Configuración
  "com.typesafe" % "config" % "1.4.3",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

// Assembly plugin para fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
