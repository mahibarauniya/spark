import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object Program9_Inputonerow_Outputmany_row extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "program for split one row to many rows")   
    val inputFile = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 10 - Apache Spark - in Depth/dataset/bigdatacampaigndata.csv")
    //println("Input file is: ")
   // inputFile.collect.foreach(println)
  
   val mappedTwoFields = inputFile.map( x => ( x.split(",")(10).toFloat,  x.split(",")(0) ))
   val flatteredData = mappedTwoFields.flatMapValues( a  =>   a.split(" ") )
   val flatteredData1= flatteredData.map(x =>(x._2, x._1))
   val flatteredData2=  flatteredData1.reduceByKey( (x,y) => x + y ).sortBy(x => x._2, false)
   flatteredData2.collect.foreach(println)
}