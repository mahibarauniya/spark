import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Program2_wordcount_caseInsensitive extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext ("local[*]", "word Count Insensitive")
  println("*************************** My first Scala program for work counting but for case insensitive strings ***************************")
  
  /*
  val inputFile = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning Material/data_set/sample_data2.txt")
  val words = inputFile.flatMap(x => x.split(" "))
  val wordCaptial = words.map(a => a.toUpperCase())
  //val wordmap= wordCaptial.map( a => (a,1))
  val wordmap= wordCaptial.map( (_,1))
  val finalWordCount = wordmap.reduceByKey( (a,b) => a + b )  
  finalWordCount.collect.foreach(println)
  */
  
 
 // above program can be written in below way also....
  
  sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning Material/data_set/sample_data2.txt").
                         flatMap(_.split(" ")).
                          map(a => a.toUpperCase()).
                          map( (_ ,1)).
                          reduceByKey( (a,b) => a + b ).
                          collect.
                          foreach(println)
 
  
}