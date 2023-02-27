package pl.wasik.stud.util

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Template matching")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
