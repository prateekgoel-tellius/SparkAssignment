import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object ListOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF Operations ")
      .master("local")
      .getOrCreate()

    val retailDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/Resource/retailanalytics_cpy.csv")

  /**
   * val listOfMeasures = List of numeric columns
   * val listOfDimensions = List of string columns
   * val listOfDates = list of date columns
   */

    val listOfMeasures = retailDF.schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map(_.name).toList
    val listOfDimensions = retailDF.schema.fields.filter(_.dataType == StringType).map(_.name).toList
    val listOfDates = retailDF.schema.fields.filter(_.dataType == TimestampType).map(_.name).toList

    println(listOfMeasures)
    println(listOfDimensions)
    println(listOfDates)

    /**
     * 1.create a dataframe with aggregation where even index columns will have sum as aggregation and odd index cols will have avg,
     * group by will be even indicies of groupby list.
     */

    val aggregationCols = listOfMeasures.zipWithIndex.map { case (col, index) =>
      if (index % 2 == 0) sum(col).alias(s"sum_${col}")
      else avg(col).alias(s"avg_${col}")
    }

    val groupByCols = listOfDimensions.zipWithIndex.collect {
      case (col, index) if index % 2 == 0 => col
    }

    val aggregatedDF = retailDF.groupBy(groupByCols.head, groupByCols.tail: _*).agg(aggregationCols.head, aggregationCols.tail: _*)
    aggregatedDF.show()

    /**
     * 2. create a dataframe with addition 3 columns for each date column from above list
     *  => day_${dateCol}, month_${dateCol}, year_${dateCol}
     */

    val dateColumnsDF = listOfDates.foldLeft(retailDF) {
      (df, dateCol) =>
        df.withColumn(s"day_$dateCol", dayofmonth(to_date(col(dateCol))))
          .withColumn(s"month_$dateCol", month(to_date(col(dateCol))))
          .withColumn(s"year_$dateCol", year(to_date(col(dateCol))))
    }
    dateColumnsDF.printSchema()
    dateColumnsDF.show()


  }
}
