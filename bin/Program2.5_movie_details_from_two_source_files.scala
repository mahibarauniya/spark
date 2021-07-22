import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Program14_movie_details_from_two_source_files extends App{
  //Logger.getLogger("org").setLevel(Level.ERROR)
  println("***************************Program for working on  two data sets (user- movie rating and movie list details ***************************")
  val sc = new SparkContext("local[*]","usermoviedata")
  //movie rating should be given by 100 users
  //movie rating should be 4.0 or above 
  
  //load the data from first file only what is needed 
  val userMovieRating   = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week11-Apache-spark-Structured_API_Part1/ratings.dat") 
  val mappedRdd = userMovieRating.map( x => {
  val fields = x.split("::")
   (fields(1), fields(2))
  } )
//input 
//(150,3)
//(150,2)
//(150,5)
  
//ouput for average 
//(150,(3,1))
//(150,(2,1))
//(150,(5,1))
  
 val mappedRdd1 = mappedRdd.mapValues(x => (x.toFloat,1.0))   
 
 //input  for average 
//(150,(3.0,1.0))
//(150,(2.0,1.0))
//(150,(5.0,1.0))
  
//Output for average 
//(150,(8.0,3)) 
 
 val mappedRdd2 = mappedRdd1.reduceByKey((x,y) => ((x._1+y._1), (x._2+y._2) ))
 
 val mappedRdd3 = mappedRdd2.filter (x=> x._2._2 > 10)  
   
//Input for average 
//(150,(4999.0,1050)) 

 //Output for average 
//(150,4.76)

 val mappedRdd4 = mappedRdd3.mapValues( x=> x._1 /x._2).filter(x => x._2 > 4.0) 
 
 
 
 
 
 //loading second data sets from second data file ...only those fields what is needed
 val movieDetails= sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week11-Apache-spark-Structured_API_Part1/movies.dat") 
 
 val movieDetailsPairedRdd= movieDetails.map( x=> {
 val fields = x.split("::")
 (fields(0), fields(1))
 } )
 
 // two rdds and now we need to join those two rdds...
 //when we join ..join happened on keys
 val joinedRdd = mappedRdd4.join(movieDetailsPairedRdd)
 
 val finalProcessedRdd= joinedRdd.map(x=> (x._2._2, x._2._1))
 finalProcessedRdd.collect.foreach(println)
 
 scala.io.StdIn.readLine()
 
 }