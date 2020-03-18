package thoughtworks.sales

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object TransformData {
  def main(args: Array[String]): Unit = {
    val arguments = args.map(e => {
      e.split("=") match {
        case Array(key, value) => key -> value
        case _ => "" -> ""
      }
    }).toMap
    val inputPath = arguments("--inputPath")
    val outputPath = arguments("--outputPath")
    val errorPath = arguments("--errorPath")
    val spark = SparkSession.builder.appName(this.getClass.getName).getOrCreate()
    run(spark, inputPath, outputPath, errorPath)
  }

  private def calculateProfit = udf((revenue: Double, cost: Double) => {
    revenue - cost
  })

  def run(spark: SparkSession, inputPath: String, outputPath: String, errorPath: String): Unit = {
    val data = spark.read.option("header", "true").csv(inputPath)
    val errorData = data.filter(col("ORDER ID").isNull)
    val transformedData = data.filter(col("ORDER ID").isNotNull).withColumn("Total Profit", calculateProfit(data("Total Revenue"), data("Total Cost")))
    transformedData.write.option("header", "true").mode(SaveMode.Overwrite).csv(outputPath)
    errorData.write.option("header", true).mode(SaveMode.Overwrite).csv(errorPath)
  }
}
