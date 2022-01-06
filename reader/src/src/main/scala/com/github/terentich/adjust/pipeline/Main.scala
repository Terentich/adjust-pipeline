package com.github.terentich.adjust.pipeline

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.IntegerType

object Main {
  private implicit lazy val logger: Logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.
      builder()
      .appName(s"Adjust reader")
      .getOrCreate()

    val dataDf = spark
      .read.format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", spark.conf.get("spark.db.url"))
      .option("user", spark.conf.get("spark.db.user"))
      .option("password", spark.conf.get("spark.db.password"))
      .option("dbtable", "igra_data")
      .option("partitionColumn", "day")
      .option("lowerBound", "1")
      .option("upperBound", "31")
      .option("numPartitions", 30)
      .load()

    val outputDf = dataDf
      .withColumn("thousands_altitude",
        when(col("gph") % 1000 === 0, col("gph") / 1000)
          .otherwise(col("gph") / 1000 + 1)
          .cast(IntegerType) * 1000
      )
      .repartition(col("thousands_altitude"))

    outputDf
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("thousands_altitude")
      .parquet(spark.conf.get("spark.data.output"))
  }
}
