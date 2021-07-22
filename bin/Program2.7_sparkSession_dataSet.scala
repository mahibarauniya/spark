import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.Timestamp


object Program16_sparkSession_dataSet extends App{
	
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "My first Spark session Program")
  sparkconf.set("spark.master", "local[2]") 
  
  val spark = SparkSession.builder()
  .config(sparkconf)
  .getOrCreate()
  
 //val orderDf = spark.read.csv("C:/Users/Barauniya/Desktop/BIG DATA/Week11-Apache-spark-Structured_API_Part1/orders.csv")
  
  case class OrdersData(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)
 
  val orderDf  : Dataset [Row] =  spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("C:/Users/Barauniya/Desktop/BIG DATA/Week11-Apache-spark-Structured_API_Part1/orders.csv")
 
  import spark.implicits._    //after creating spark session onlyl we can create spark import cover DF <-> DS 
  
  val orderDs = orderDf.as[OrdersData]
  
 orderDs.filter( x  =>  x.order_id  <10 ).show(10)
 //orderDf.filter("order_customer_id33 >10000").show

 
 scala.io.StdIn.readLine()
 spark.stop()
}


