//Interview question..
//from Spark DataFrame Session-15.............................

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.functions._

object Program18_scala_list_to_df_duplicate_new_column extends App{
          Logger.getLogger("org").setLevel(Level.ERROR)    
          
          val sparkconf = new SparkConf()
          sparkconf.set("spark.app.name", "My Spark  Program for adding an extra column in file ")
          sparkconf.set("spark.master", "local[2]") 
          
           val spark = SparkSession.builder()
          .config(sparkconf)
          .getOrCreate() 
          
          val mylist= List (  (1,"2013-09-09", 11599, "CLOSED"),
              (2,"2017-02-07", 1937, "CLOSED"),
              (3,"2013-09-09", 11599, "OPEN"),
              (4,"2013-09-09", 4590, "ACTIVE"))
              
         
        //val rdd = spark.SparkContext.parallelize (mylist) don't want to use rdd method
              
          
           val ordersDf = spark.createDataFrame(mylist)
           
           val inputDF= ordersDf.toDF("order_id", "order_date", "cust_id", "status")
           
           //val processedDF = inputDF.withColumn("order_date", unix_timestamp(col("order_date").cast(DataType))) change one existing column 
           
           val processedDF = inputDF.withColumn("NewId", monotonically_increasing_id)
                                                        .dropDuplicates("order_date", "cust_id")
                                                        .drop("order_id")
                                                        .sort("cust_id")
           processedDF.show()
           
  }