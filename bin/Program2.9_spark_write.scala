import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object Program20_spark_write extends App{
  
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "My first Spark session Program")
  sparkconf.set("spark.master", "local[2]") 
  
  val spark = SparkSession.builder()
  .config(sparkconf)
  .getOrCreate()
  
  print("********** Start Program: Program20_spark_write **********")
  
   //reading finished from file to DataFrame....
  val orderSchemaDDL = "orderid Int, orderdate Timestamp,ordercustomerid Int,orderstatus String"  
  
  val orderDf = spark.read
  .format("csv")
  .option("header", true)
  .schema(orderSchemaDDL)
  .option("path","C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/dataset/orders.csv")
  .load() 
  
  
  val filterOrderDf = orderDf.filter("ordercustomerid >3000")
    
  filterOrderDf.printSchema()   
  filterOrderDf.show(20)
  
  //writing the data into file from DataFrame....
  
  print("Number of patition of this data frame is: "+ filterOrderDf.rdd.getNumPartitions)
  
  //val repDf =  filterOrderDf.repartition(4)
  
  //print("Number of patition of this data frame is: "+ repDf.rdd.getNumPartitions)
  //repDf.write
  
  
  filterOrderDf.write
  .format("csv")  //.format("json")
  .mode(SaveMode.Append)
  .partitionBy("orderstatus")
  .option("path","C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/")
  .save()
  
  
    
}