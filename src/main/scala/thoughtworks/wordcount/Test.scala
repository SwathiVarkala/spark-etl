package thoughtworks.wordcount


import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Word Count").getOrCreate()
    import spark.implicits._
    println("Application Initialized: " + spark.sparkContext.appName)
    println("process started")
    val df = Seq(1,2,3,4,5).toDF()
    val rddFiltered = df.filter(entry => entry.getAs[Int](0) > 3)
    println(rddFiltered.collectAsList().toString)
    println("process ended")
    spark.stop()
  }
}
