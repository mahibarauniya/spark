import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Program3_top10_words extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "program for top 10 words") 
  
  val inputFile = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning Material/data_set/sample_data2.txt")
  // there is no transformation of sortByValue but we do have transformation with sortByKey....
  
  val words = inputFile.flatMap(a => a.split(" "))
  val capWords = words.map(_.toUpperCase() ).map( a => (a,1)).reduceByKey( (a,b) => a + b )
  
  val shuffled_word = capWords.map( x => (x._2, x._1))
  val desc_sorted_result = shuffled_word.sortByKey(false).map(x => (x._2, x._1))
  
  val results = desc_sorted_result.collect
  for (result <- results) {
    val word = result._1
    val count = result._2 
    
    println(s"$word: $count")
  }
  
}