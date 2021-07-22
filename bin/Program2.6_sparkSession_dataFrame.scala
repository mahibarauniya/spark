import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Program15_sparkSession_dataFrame extends App{
	
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "My first Spark session Program")
  sparkconf.set("spark.master", "local[2]") 
  
  val spark = SparkSession.builder()
  .config(sparkconf)
  .getOrCreate()
  
 //val orderDf = spark.read.csv("C:/Users/Barauniya/Desktop/BIG DATA/Week11-Apache-spark-Structured_API_Part1/orders.csv")
 
  val orderDf  =  spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("C:/Users/Barauniya/Desktop/BIG DATA/Week11-Apache-spark-Structured_API_Part1/orders.csv")
  
  orderDf.printSchema() 
  
   val groupOrderDf  = orderDf
  .repartition(4)
  .where("order_customer_id >10000")
  .select("order_id", "order_customer_id","order_status")
  .groupBy("order_customer_id")
  .count()
 
 groupOrderDf.show(50)
 
 scala.io.StdIn.readLine()
 spark.stop() 
}