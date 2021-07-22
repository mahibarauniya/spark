import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Program24_logging_example  extends App{
  
          case class Logging( Level: String, datetime: String)
          
          
         def mapper(line: String) : Logging = {
            val fields = line.split(',')
            val logging: Logging = Logging (fields(0), fields(1))
            return logging
          }
          
          
          
          Logger.getLogger("org").setLevel(Level.ERROR)    
          
          val sparkconf = new SparkConf()
          sparkconf.set("spark.app.name", "My Spark  Program for adding an extra column in file ")
          sparkconf.set("spark.master", "local[2]") 
          
           val spark = SparkSession.builder()
          .config(sparkconf)
          .getOrCreate() 
          
           //If there is data in the inputted file that cannot have a schema applied to it, it will return Null for ALL the data in your table. For example: "1n" is impossible to convert to integer. If a DataTypes.IntegerType is applied to the column that contains "1n", then the whole table with have null values."
          //We have two data sets customers and order and we want to join these two data sets together....
          
          
          /*Input
          level,datetime
					DEBUG,2015-2-6 16:24:07
					WARN,2016-7-26 18:54:43
					INFO,2012-10-18 14:35:19
					*/
          
          /*output 5 log level and 12 months so output lines should be 5*12 = 60 rows....
           * 
           * DEBUG January 20007
           * WARN January 14352
           * INFO January 46858
           * * */
           
          //always start from a very small data sets and then work with large file          
         
          /*
          import spark.implicits._
          val templist = List("DEBUG,2015-2-6 16:24:07",
                                        "WARN,2016-7-26 18:54:43",
                                        "INFO,2012-10-18 14:35:19",
                                        "DEBUG,2012-4-26 14:26:50",
                                         "DEBUG,2013-9-28 20:27:13",
                                         "INFO,2017-8-20 13:17:27",
                                         "INFO,2015-4-13 09:28:17",
                                         "DEBUG,2015-7-17 00:49:27",
                                         "DEBUG,2014-7-26 02:33:09"
                                         )
          
          val rawrdd = spark.sparkContext.parallelize(templist)
          
          val rdd2 = rawrdd.map(mapper)
          
          val df1 = rdd2.toDF()
          
          df1.show()
         
          df1.createOrReplaceTempView("logging_table")
          spark.sql("select * from logging_table ").show()       // spark.sql returns a dataframe to .show()
          
          
          //spark.sql("select Level, collect_list(datetime) from logging_table group by Level  order by Level ").show(false)   /// show(false) to see the complete list on console 
          
          spark.sql("select Level, date_format(datetime, 'MMM') as  month, count(1) from logging_table group by Level, date_format(datetime, 'MMM')  order by Level ").show()
          */
          
          //working with actual file ....
          print("Starts reading from file.....")
          
          val logDataSchemaDDL = """    level String,
                                                              datetime  String """
          val logDF = spark.read.
                              format("csv").
                              option("path", "C:/Users/Barauniya/Desktop/BIG DATA/Week12 - Apache Spark - Structured API Part-2/dataset/biglog.txt"). 
                              option("header", true).
                              option("inferschema", false).
                              option("schema", logDataSchemaDDL).
                              load()
                              
            logDF.printSchema()
                              
            logDF.createOrReplaceTempView("logging_table")
           
            //spark.sql("select level, date_format(datetime, 'MMM') as  month, count(1) as total_cnt from logging_table group by level, date_format(datetime, 'MMM')   order by level ").show(false)
             val results = spark.sql("select level, " +
                                                    "date_format(datetime, 'MMM') as  month, " +
                                                    "cast(date_format(datetime, 'M')   as int ) as  month_no, " + 
                                                    "count(1) as total_cnt " +
                                                            "from logging_table " +
                                                    "group by level, date_format(datetime, 'MMM'),  " + 
                                                    "cast(date_format(datetime, 'M')   as int) " + 
                                                    "order by level ")
    
             results.createOrReplaceTempView("results_tables")
             
             spark.sql(" select  level, month, total_cnt from results_tables order by level, month_no ")     
            
             
             // code to pivot the result and output will be level and months will be in multiple columns 
              spark.sql("select level, " +
                              "date_format(datetime, 'MMM') as  month, " +
                              "cast(date_format(datetime, 'M')   as int ) as  month_no " + 
                              " from logging_table " )
                              .groupBy("level").pivot("month_no").count() //.show()
              
                                    
               //Optimized Query for pivoting pass prespecified list of values as List into Pivot function
                                                     
               val monthsList = List("January", "February", "March", "April", "May", "June", "July", "August","September", "October", "November","December")
               
               spark.sql("select level, " +
                                                    //"date_format(datetime, 'MMM') as  month, " +
                                                    " date_format(datetime, 'MMMM')  as  month " + 
                                                    " from logging_table " ).groupBy("level").pivot("month",  monthsList).count().show(100)
 
             scala.io.StdIn.readLine()
             spark.stop()               
        }