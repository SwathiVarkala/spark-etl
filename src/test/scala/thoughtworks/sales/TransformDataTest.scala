package thoughtworks.sales

import java.nio.file.{Files, StandardOpenOption}

import thoughtworks.DefaultFeatureSpecWithSpark

class TransformDataTest extends DefaultFeatureSpecWithSpark {
  feature("Sales data Ingestion") {
    scenario("Add profit column") {
      Given("Sales file present in input folder")

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)

      val inputCsv = Files.createFile(rootDirectory.resolve("input.csv"))
      val outputDirectory = rootDirectory.resolve("output")
      import scala.collection.JavaConverters._
      val lines = List(
        "Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost",
        "Australia and Oceania,Tuvalu,Baby Food,Offline,H,5/28/2010,669165933,6/27/2010,9925,255.28,159.42,2533654.00,1582243.50",
        "Central America and the Caribbean,Grenada,Cereal,Online,C,8/22/2012,963881480,9/15/2012,2804,205.70,117.11,576782.80,328376.44"
      )
      Files.write(inputCsv, lines.asJava, StandardOpenOption.CREATE)

      When("Ingestion runs")
      TransformData.run(spark, inputCsv.toUri.toString, outputDirectory.toUri.toString)

      val actual = spark.read.option("header", "true").csv(outputDirectory.toUri.toString)
      Then("Profit column has been added")
      println(actual.columns)
      val expectedColumns = Array("Region", "Country", "Item Type", "Sales Channel", "Order Priority", "Order Date", "Order ID", "Ship Date", "Units Sold", "Unit Price", "Unit Cost", "Total Revenue", "Total Cost", "Total Profit")
      actual.columns should be(expectedColumns)

      And("Output has been generated and stored in output folder")
      import spark.implicits._
      val expected = List(("Australia and Oceania", "Tuvalu", "Baby Food", "Offline", "H", "5/28/2010", "669165933", "6/27/2010", "9925", "255.28", "159.42", "2533654.00", "1582243.50", "951410.5"), ("Central America and the Caribbean", "Grenada", "Cereal", "Online", "C", "8/22/2012", "963881480", "9/15/2012", "2804", "205.70", "117.11", "576782.80", "328376.44", "248406.36000000004")).toDF(expectedColumns: _*)
      actual.collect() should contain theSameElementsAs expected.collect()
    }
  }
}
