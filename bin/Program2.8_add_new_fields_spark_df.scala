import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.functions._


object Program17_add_new_fields_spark_df extends App{
  
          Logger.getLogger("org").setLevel(Level.ERROR)   
          
          def salaryCheck( salary: Int ) : String ={
            if (salary >= 10000) "Y" else "N"
          }
          
          val sparkconf = new SparkConf()
          sparkconf.set("spark.app.name", "My Spark  Program for adding an extra column in file ")
          sparkconf.set("spark.master", "local[2]") 
          
          case class empData(name : String, salary : Int, deptno : Int)
          
          val spark = SparkSession.builder()
          .config(sparkconf)
          .getOrCreate() 
          
          import spark.implicits._    // DF <-> DS 
          
          val empDf = spark.read
          .format("csv")
          .option("inferSchema", true)
          //.option("header", true) since no header is there in the file so column will be like _c0, _c1, _c2
          .option("path", "C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/dataset/employee.csv")
          .load()
          
          val df1 : Dataset[Row] = empDf.toDF("name", "salary", "deptno")
          
          val parseSalaryFunction = udf( salaryCheck ( _ : Int ) : String)
          
          val df2 = df1.withColumn("salaryFlag", parseSalaryFunction(col("salary")) )
          
          df2.printSchema()          
          df2.show()
          
          //          //just for practice converting data frame to data set
          //          print("Converting the data frame to data sets....")
          //          val empDs = df1.as[empData]       
          //        
          //          
          //          //converting data set to dataframe
          //          val empDf1 = empDs.toDF()        
          //            empDf1.show()
          
          // In above lines we were using column objects notion 
          //now we are going to use SQL expression udf to add a new column in the df which is easier ... 
          //val df3 = df1.withColumn("salaryFlagN", expr("parseSalaryFunctionSQL(salary)")   )
          
          //df3.show()   
          
          
         //better way to register the function in catalog and then use the same 
          spark.udf.register("parseSalaryFunctionSQL", salaryCheck ( _ : Int ) : String)
          
          spark.catalog.listFunctions().filter( x => x.name == "parseSalaryFunctionSQL").show()
          
          df1.createOrReplaceTempView("empTable")
          
          spark.sql("select name, salary, deptno, parseSalaryFunctionSQL(salary) as salary_flag from  empTable ").show()    
          
    
          
}