import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.expressions.Window


object Program23_Window_Aggregate extends App{
          Logger.getLogger("org").setLevel(Level.ERROR)    
          
          val sparkconf = new SparkConf()
          sparkconf.set("spark.app.name", "My Spark  Program for adding an extra column in file ")
          sparkconf.set("spark.master", "local[2]") 
          
           val spark = SparkSession.builder()
          .config(sparkconf)
          .getOrCreate() 
          
          //If there is data in the inputted file that cannot have a schema applied to it, it will return Null for ALL the data in your table. For example: "1n" is impossible to convert to integer. If a DataTypes.IntegerType is applied to the column that contains "1n", then the whole table with have null values."
          
    val orderDataSchemaDDL = """ country String, 
          weeknum Int,
          numinvoice Int,
          totalquantity Int,
          invoicevalue Float
          """
          
    val orderDf = spark.read
           .format("csv")
           .option("header", true)
           .schema(orderDataSchemaDDL)
          // .option("inferSchema", true) ..not good in production ...
           .option("delimiter", ",")
           .option("path", "C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/dataset/windowdata.csv")
           .load() 
            
           
           //we are looking for running total..
           //we need to work on: grouping or paritition column ..ordering column .... window size 
           
           val mywindow = Window.partitionBy("country")
           .orderBy("weeknum")
           .rowsBetween(Window.unboundedPreceding, Window.currentRow)
           
           
           orderDf.withColumn("runningtotal", sum("invoicevalue").over(mywindow) ).show()
           
           
           orderDf.printSchema()
           orderDf.show(10)
           
           //First Way: Column object expression ....
         
           scala.io.StdIn.readLine()
           spark.stop()               
        }