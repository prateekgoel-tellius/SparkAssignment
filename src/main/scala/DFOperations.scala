import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, sum, year}

import java.nio.file.{Files, Paths}
import java.nio.file.StandardCopyOption

object DFOperations {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF Operations ")
      .master("local")
      .getOrCreate()
    // 1. Load a dataframe from a csv file (retail csv)
    var retailDF = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/Resource/retailanalytics_cpy.csv")

    //2. print schema and make sure first row which is header in csv file is considered.
    retailDF.printSchema()

    // 3. cast a column from integer to double
      retailDF = retailDF.withColumn("Quantity", col("Quantity").cast("Double"))
    retailDF.printSchema()

    //4. add a new column which is sum of any 3 numeric columns
      retailDF = retailDF.withColumn("SpecialSum", col("Discount") + col("Profit") + col("Shipping_Cost"))
    retailDF.show()

    //5. delete any one of column
      retailDF = retailDF.drop("Order_Priority")
      retailDF.printSchema()

    //6. print logical plan and check above steps will be present
      retailDF.explain(true)

    //7. print some sample rows
      retailDF.show()

    //8. move file from given location and again try println sample rows you will get error saying file not found
    val sourcePath = Paths.get("src/Resource/retailanalytics_cpy.csv")
    val destinationPath = Paths.get("src/ChangedDirectory/retailanalytics_cpy.csv")
    Files.move(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING)
    retailDF.show() //this will throw the FileNotFoundException

    //9. move back file to correct place and cache above dataframe
    Files.move(destinationPath, sourcePath, StandardCopyOption.REPLACE_EXISTING)
    retailDF.cache()
    retailDF.show()

    //10. now perform action like count
    val rowCount = retailDF.count()
    println("Number of rows in Retail CSV : "+ rowCount)

    //11. move file from given location and again try println sample rows , error wont be thrown now because data is present in memory
    Files.move(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING)
    retailDF.show()

    //12. filter above rows for only 2013.
    val data2013 = retailDF.filter(year(col("Order_Date") )=== 2013)
    data2013.show()

    //print sum of sales for each ship mode
    val sumOfSalesGroupByShipMode = retailDF.groupBy("Ship_Mode").agg(sum("Sales").as("Total_Sales"))
    sumOfSalesGroupByShipMode.show()

    //print sum of sales and discount for each ship mode and category(aggregations => sum sales, sum discount, groupBy => ship mode and category)

    val sumOfSalesandDiscountGroupByShipMode = retailDF.groupBy("Ship_Mode").agg(sum("Sales").as("Total_Sales"),sum("Discount").as("Total_Discount"))
    sumOfSalesandDiscountGroupByShipMode.show()

    //write above dataframes to a csv file.
    sumOfSalesGroupByShipMode.coalesce(1).write.option("header", "true").mode("overwrite").csv("src/Resource/sumSalesByShipMode.csv")
    sumOfSalesandDiscountGroupByShipMode.coalesce(1).write.option("header", "true").mode("overwrite").csv("src/Resource/sumSalesDiscountByShipMode.csv")

  }
}
