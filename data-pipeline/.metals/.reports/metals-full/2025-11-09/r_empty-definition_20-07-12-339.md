error id: file://<WORKSPACE>/src/main/scala/com/deltaTripNyc/DeltaTripNyc.scala:`<none>`.
file://<WORKSPACE>/src/main/scala/com/deltaTripNyc/DeltaTripNyc.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -com/typesafe/scalalogging/LazyLogging#
	 -LazyLogging#
	 -scala/Predef.LazyLogging#
offset: 142
uri: file://<WORKSPACE>/src/main/scala/com/deltaTripNyc/DeltaTripNyc.scala
text:
```scala
package com.deltaTripNyc

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object DeltaTripNyc extends L@@azyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("Starting Data Pipeline.")

    val spark: SparkSession = SparkSession.builder()
      .appName("DeltaTripNYC")
      .getOrCreate()

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

```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.