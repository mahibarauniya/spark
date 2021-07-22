import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Program5_customer_movie_rating extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
   val sc        = new SparkContext("local[*]","customer data") 
  //Program to find out how many times movie were rated 5 star 
  //Program to find out how many times movie were rated 4 star 
  //Program to find out how many times movie were rated 3 star 
  //Program to find out how many times movie were rated 2 star 
  //Program to find out how many times movie were rated 1 star 
  //dataset : customer_id <tab delimeted> movie_id <tab delimeted>  rating <tab delimeted>  time
   

   val inputData   = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning Material/data_set/moviedata.dat")     // creating base RDD by loading the file
 /*
         val mappedInputData = inputData.map(x => x.split("\t")(2))
         val tupleRating = mappedInputData.map(x => (x,1) )
         val cntAggRating = tupleRating.reduceByKey((x,y) => x+y)
         val sortCntAggRating = cntAggRating.sortBy(x => x._1)
         
         /result
          val result  = sortCntAggRating.collect()
          result.foreach(println)   
 */
  
   // same program by  using countByValue ...and countByValue gives an local variable and this is Action ...not transformation ...
   val mappedInputData   = inputData.map(x => x.split("\t")(2))
   val cntMovieRating       = mappedInputData.countByValue   
   
   cntMovieRating.foreach(println)

}