import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source



object Program10_broadcastVariables extends App{
  
  def loadBoringWords() : Set[String] ={
    var boringWords : Set[String] = Set() //empty set 
    val lines = Source.fromFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 10 - Apache Spark - in Depth/dataset/boringwords.txt").getLines()
    for (line <- lines){
      boringWords += line
    }
    boringWords
  }
  
  
  
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "program for split one row to many rows")  
    var nameSet = sc.broadcast(loadBoringWords)
    
    
    
    val inputFile = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 10 - Apache Spark - in Depth/dataset/bigdatacampaigndata.csv")
    println("Input file is: ")
   inputFile.collect.foreach(println)
  
   val mappedTwoFields = inputFile.map( x => ( x.split(",")(10).toFloat,  x.split(",")(0) ))
   val flatteredData  = mappedTwoFields.flatMapValues( a  =>   a.split(" ") )
   val flatteredData1= flatteredData.map(x =>(x._2, x._1))
   
   
   
   val filteredFinalMapped   = flatteredData1.filter( x => !nameSet.value(x._1))   
   val flatteredData2 =  filteredFinalMapped.reduceByKey( (x,y) => x + y ).sortBy(x => x._2, false)
   flatteredData2.collect.foreach(println)
}