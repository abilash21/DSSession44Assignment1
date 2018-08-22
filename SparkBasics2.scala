package SparkAssignment2
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import scala.io.Source

object SparkBasics2 {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g") //Configuration
    val sc=new SparkContext(conf) // Create Spark Context with Configuration
    val spark = SparkSession //Create Spark Session
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
  
    val dfHoliday = spark.read.option("header", "false").csv("Dataset_Holidays.txt") //Read the Input Data
    .toDF("user_id", "src", "dest", "travel_mode", "distance", "year_of_travel") // Add Header
    dfHoliday.show() // Show all data
    dfHoliday.groupBy("year_of_travel").count().show() // To get no of travelers per year, user of Group By and Count
    // Group by User ID and Year and use Sum function to aggregate distance
    dfHoliday.groupBy("user_id","year_of_travel").agg(sum("distance")).show() 
    //Group By User ID, Aggregate the distance (TotalDistance), Order by descending TotalDistance
    dfHoliday.groupBy("user_id").agg(sum("distance") as "TotalDistance").orderBy(desc("TotalDistance")).show()
    //Group By Destination and count the number of people visited the destination
    dfHoliday.groupBy("dest").count().orderBy(desc("count")).show()
    println("Completed")
  }
}