package SparkAssignment
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source


object SparkBasics {
  
  
def main(args: Array[String]): Unit = {
// Create Spark Configuration
  val conf:SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
  val sc=new SparkContext(conf) // Create Spark Context with Configuration
  var bRDD = sc.textFile("Dataset.txt") //Create RDD from input data
  val header = bRDD.first()
  bRDD = bRDD.filter(row => row != header)
  val tupRDD = bRDD.map(x=> (x.split(",")(0),x.split(",")(1))) //Use Map and Split function to create the Tuple
  tupRDD.foreach(println)
  println("No of Lines in the file excluding Header: " + bRDD.count())
  val disRDD = bRDD.map(x=> (x.split(",")(1),1)).reduceByKey((x,y)=>(x+y)) //Use split and constant value 1 to create a tuple and use reducebykey
  println("#############################################")
  println("Distinct Subject")
  disRDD.foreach(println)
  println("#############################################")
  val RDD1 = bRDD.map(x=> ((x.split(",")(0),x.split(",")(3).toInt),1)) // get Name and Mark from the Source RDD
  val RDD1Filter = RDD1.filter(x=> x._1._1 == "Mathew" && x._1._2 ==55) // apply filter and create a RDD to get only Mathew with mark 55
  println("No of Records wih name Mathew and mark 55") 
  val RDD1reduce = RDD1Filter.reduceByKey((x,y)=>x+y).foreach(println) // Print and get the number of count of the RDD 
  val RDD2reduce = bRDD.map(x=> (x.split(",")(2),1)).reduceByKey((x,y)=>x+y).foreach(println) //Count of Students per Grade
  
  val RDD3 = bRDD.map(x=> ((x.split(",")(0),x.split(",")(2)),x.split(",")(3).toInt)) // Get Name, Grade and Mark
  val RDD3Map = RDD3.mapValues(x=>(x,1)) // for the values that is numeric add counter 1 for each
  val RDD3reduce = RDD3Map.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // sum the values of mark and the counter 
  val StuAvg = RDD3reduce.mapValues{case(sum,count)=>(1.0*sum)/count} //divide sum / count to get average
  println("#############################################")
  StuAvg.foreach(println)
  
  val RDD4 = bRDD.map(x=> ((x.split(",")(0),x.split(",")(1)),x.split(",")(3).toInt)) // Get Name, Subject and Mark
  val RDD4Map = RDD4.mapValues(x=>(x,1)) // for the values that is numeric add counter 1 for each
  val RDD4reduce = RDD4Map.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // sum the values of mark and the counter 
  val StuSubjAvg = RDD4reduce.mapValues{case(sum,count)=>(1.0*sum)/count} //divide sum / count to get average
  println("#############################################")
  StuSubjAvg.foreach(println)
  
  val RDD5 = bRDD.map(x=> ((x.split(",")(1),x.split(",")(2)),x.split(",")(3).toInt)) // Get Grade, Subject and Mark
  val RDD5Map = RDD5.mapValues(x=>(x,1)) // for the values that is numeric add counter 1 for each
  val RDD5reduce = RDD5Map.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // sum the values of mark and the counter 
  val GradeSubjAvg = RDD5reduce.mapValues{case(sum,count)=>(1.0*sum)/count} //divide sum / count to get average
  println("#############################################")
  GradeSubjAvg.foreach(println)
  
  val RDD6 = bRDD.map(x=> ((x.split(",")(0),x.split(",")(2)),x.split(",")(3).toInt)) // Get Name, Grade and Mark
  val RDD6Map = RDD6.mapValues(x=>(x,1)) // for the values that is numeric add counter 1 for each
  val RDD6reduce = RDD6Map.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // sum the values of mark and the counter 
  val AvgGrade = RDD6reduce.mapValues{case(sum,count)=>(1.0*sum)/count} //divide sum / count to get average
  println("#############################################")
  AvgGrade.filter(x=>x._1._2 == "grade-2" && x._2>50).foreach(println)
  
  val RDD7 = bRDD.map(x=> (x.split(",")(0),x.split(",")(3).toInt)) // Get Name and Mark
  val RDD7Map = RDD7.mapValues(x=>(x,1)) // for the values that is numeric add counter 1 for each
  val RDD7Reduce = RDD7Map.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // sum the values of mark and the counter
  val AvgStudent = RDD7Reduce.mapValues{case(sum,count)=>(1.0*sum)/count} //divide sum / count to get average
  
  val flatAvgGrade = AvgGrade.map(x=> (x._1._1 ,x._2.toDouble)) // form the flat Avg with name by removing grade
  val commanVal = flatAvgGrade.intersection(AvgStudent) // use intersection to get the comman values between flatAvgGrade and AvgStudent
  println("#############################################")
  commanVal.foreach(println) 
  println("Looks like no comman values")
  
  println("Success")
}
}