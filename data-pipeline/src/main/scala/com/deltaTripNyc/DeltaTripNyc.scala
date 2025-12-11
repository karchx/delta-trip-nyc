package com.deltaTripNyc

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.commons.io.FileUtils
import java.io.File
import io.delta.tables._

object DeltaTripNyc extends LazyLogging {
   val spark: SparkSession = SparkSession.builder()
     .appName("DeltaTripNYC")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
     .getOrCreate()

   def loadDataSource(url: String): DataFrame = {
      val df_ = spark.read.parquet(url)
      df_
   }

   def writeDelta(filePath: String, df: DataFrame): Unit = {
     df.write
        .format("delta")
        .mode("overwrite")
        .save(filePath)
   }

   def writeApproach(df: DataFrame, filePath: String): Unit = {
      val file = new File(filePath)
      if (file.exists()) FileUtils.deleteDirectory(file)

      println("Creating Delta table...")
      val path = file.getCanonicalPath
      writeDelta(path, df)
      println("Reading Delta table...")
      val dfDelta = DeltaTable.forPath(path)
      dfDelta.toDF.show(5)
   }

  def main(args: Array[String]): Unit = {
    logger.info("Starting Data Pipeline.")

    println(s"Spark version: ${spark.version}")

    val taxi_url = "file:///opt/spark/data/yellow_tripdata_2024-06.parquet"
    val dfLoad = loadDataSource(taxi_url)
    val df = dfLoad.limit(10000)
    writeApproach(df, "/opt/spark/data/tmp/taxi_delta_table")

    spark.stop()
  }
}
