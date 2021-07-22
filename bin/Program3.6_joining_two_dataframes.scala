import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._




object Program24_joining_two_dataframes  extends App{
          Logger.getLogger("org").setLevel(Level.ERROR)    
          
          val sparkconf = new SparkConf()
          sparkconf.set("spark.app.name", "My Spark  Program for adding an extra column in file ")
          sparkconf.set("spark.master", "local[2]") 
          
           val spark = SparkSession.builder()
          .config(sparkconf)
          .getOrCreate() 
          
        //If there is data in the inputted file that cannot have a schema applied to it, it will return Null for ALL the data in your table. For example: "1n" is impossible to convert to integer. If a DataTypes.IntegerType is applied to the column that contains "1n", then the whole table with have null values."
          //We have two data sets customers and order and we want to join these two data sets together....
          
      val orderDataSchemaDDL = """ order_id Int, 
            order_date String,
            order_customer_id  Int,
            order_status String
            """
        
      val customerDataSchemaDDL = """customer_id Int,
        customer_fname String,
        customer_lname String,
        customer_email String, 
        customer_password String, 
        customer_street String, 
        customer_city String, 
        customer_state String, 
        customer_zipcode Int
      """  
    
    
     val orderDf = spark.read
           .format("csv")
           .option("header", true)
           .schema(orderDataSchemaDDL)
           .option("delimiter", ",")
           .option("path", "C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/dataset/orders.csv")
           .load() 
    
     val customerDf = spark.read
           .format("csv")
           .option("header", true)
           .schema(customerDataSchemaDDL)
          // .option("inferSchema", true) ..not good in production ...
           .option("delimiter", ",")
           .option("path", "C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/dataset/customers.csv")
           .load() 
          
           val customerNewDf = customerDf.withColumnRenamed("customer_id", "cust_id") // just in case 
           
           orderDf.show(10)
           
           customerNewDf.show(10)  
           
           
           val joincondition = orderDf.col("order_customer_id") === customerNewDf.col("cust_id")
           val jointype = "inner"
           
           /*orderDf.join(customerNewDf,joincondition , jointype ).
           select ("cust_id", "customer_fname", "customer_lname", "order_id", "order_status").
           show()
           */
           orderDf.join(customerNewDf,joincondition , jointype ).
           select ("cust_id", "customer_fname", "customer_lname", "order_id", "order_status").
           sort("order_id").
           withColumn("order_id", expr("coalesce(order_id, -1)")).   //to put default value into it...
           show(1000)
           
           //orderDf.join(customerDf, orderDf.col("order_customer_id") === customerDf.col("customer_id"), "inner" )
           
           
           
          
           scala.io.StdIn.readLine()
           spark.stop()               
        }