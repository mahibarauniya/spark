import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object Program1_wordcount extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  println("*************************** My first Scala program for work counting ***************************")
  val sc = new SparkContext("local[*]", "wordCount")
  
  val input = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning and code/data_set/search_data.txt")
  val words = input.flatMap(x => x.split(" "))
  val workMap = words.map(a => (a,1))
  val word_final_count = workMap.reduceByKey( (a,b) => a + b )
  word_final_count.collect.foreach(println)   
  
  scala.io.StdIn.readLine() // to see dag on local host localhost:4040 or localhost:4041 
  
} 