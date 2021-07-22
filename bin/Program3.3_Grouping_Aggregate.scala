import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum


object Program23_Grouping_Aggregate extends App{
          Logger.getLogger("org").setLevel(Level.ERROR)    
          
          val sparkconf = new SparkConf()
          sparkconf.set("spark.app.name", "My Spark  Program for adding an extra column in file ")
          sparkconf.set("spark.master", "local[2]") 
          
           val spark = SparkSession.builder()
          .config(sparkconf)
          .getOrCreate() 
          
          //If there is data in the inputted file that cannot have a schema applied to it, it will return Null for ALL the data in your table. For example: "1n" is impossible to convert to integer. If a DataTypes.IntegerType is applied to the column that contains "1n", then the whole table with have null values."
          
    val orderDataSchemaDDL = """ InvoiceNo Int, 
          StockCode Int,
          Description String,
          Quantity Int,
          InvoiceDate String,
          UnitPrice Float,
          CustomerID Int,
          Country String
          """
          
    val orderDf = spark.read
           .format("csv")
           .option("header", true)
           .schema(orderDataSchemaDDL)
          // .option("inferSchema", true) ..not good in production ...
           .option("delimiter", ",")
           .option("nullValue", "null")
           .option("path", "C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/dataset/order_data.csv")
           .load() 
            
           orderDf.printSchema()  
           
           //First Way: Column object expression ....
           orderDf.groupBy( "Country", "InvoiceNo")
           .agg(sum("Quantity").as("TotalQuantity"),
                   sum("UnitPrice").as("UnitPrice") 
                   ).show()
               
               
            //Second Way:: String object expression ....             
//               orderDf.groupBy( "Country", "InvoiceNo")
//               .agg("sum(Quantity) as TotalQuantity", "sum(UnitPrice) as UnitPrice"
//               ).show()
//               
//                         
               
               
              //Third Way:: Spark SQL.....               
               orderDf.createOrReplaceTempView("InvoiceOrderDetails")         
               spark.sql("select Country, InvoiceNo, sum(Quantity), sum(UnitPrice) from InvoiceOrderDetails group by Country, InvoiceNo").show()
  
               scala.io.StdIn.readLine()
               spark.stop()
               
        }