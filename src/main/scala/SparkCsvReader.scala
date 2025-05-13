package com.binvij

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SparkCsvReader(fileName: String) {
  private val sparkSession = SparkSession
    .builder()
    .appName("CSVReader")
    .master("local[*]")
    .getOrCreate()

  private def readCsv(): Option[DataFrame] = {
    Some(
      sparkSession.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(fileName)
    )
  }

  def fromCsvToJson(): Future[Array[String]] = Future{
    // convert each row to JSON string
    val csvDf = readCsv() match {
      case Some(x) => x
      case _       => throw new RuntimeException("Could not read csv file")
    }
    val jsonRows = csvDf.toJSON.collect()
    sparkSession.stop()
    jsonRows
  }
}

object SparkCsvReader {
  def apply(fileName: String): SparkCsvReader = {
    new SparkCsvReader(fileName)
  }
}

