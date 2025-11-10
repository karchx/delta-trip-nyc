package com.deltaTripNyc

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object DeltaTripNyc extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("Starting Data Pipeline.")

    val spark: SparkSession = SparkSession.builder()
      .appName("DeltaTripNYC")
      .getOrCreate()

    println(s"Spark version: ${spark.version}")

    import spark.implicits._
    val data = Seq(
      ("2024-01-01", 100),
      ("2024-01-02", 150),
      ("2024-01-03", 200)
    )

    val df = data.toDF("date", "trips")
    df.show()

    spark.stop()
  }
}
