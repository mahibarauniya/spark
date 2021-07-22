import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Program21_spark_sql  extends App{
   val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "My first Spark session Program")
  sparkconf.set("spark.master", "local[2]") 
  
  val spark = SparkSession.builder()
  .config(sparkconf)
  .getOrCreate()
  
  print("********** Start Program: Program21_spark_sql **********")
  val orderSchemaDDL = "orderid Int, orderdate Timestamp,ordercustomerid Int,orderstatus String"  
  
  val orderDf = spark.read
  .format("csv")
  .option("header", true)
  .schema(orderSchemaDDL)
  .option("path","C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/dataset/orders.csv")
  .load() 
  
  orderDf.createOrReplaceTempView("orderDf")  
  val sqlResultDf = spark.sql("select orderstatus, count(1) as TOTAL_STATUS from orderDf group by orderstatus order by orderstatus asc")
  
  sqlResultDf.show(50)  
  
  scala.io.StdIn.readLine()
  spark.stop() 
}