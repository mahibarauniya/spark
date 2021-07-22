import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import breeze.linalg.min


object Program8_min_temp_each_Station extends App{
      Logger.getLogger("org").setLevel(Level.ERROR)
      val sc        = new SparkContext("local[*]","minimum temp of station")
      val input   = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning Material/data_set/tempdata.csv")     // creating base RDD by loading the file
      val filterData = input.filter( x => x.split(",")(2) == "TMIN")
      val stationTuple = filterData.map(x => (x.split(",")(0), x.split(",")(3).toFloat))
      
      val mappedInput = stationTuple.reduceByKey( (x,y) => min(x,y))     
      println("Each Station minimum temperature is below: ")
      mappedInput.collect.foreach(println)  
}