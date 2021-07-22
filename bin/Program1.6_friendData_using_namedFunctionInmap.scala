import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Program6_friendData extends App{
  
    def parseLine ( line : String ) ={
      val fields = line.split("::")
      val age = fields(2).toInt
      val noOfConnection = fields(3).toInt
      (age, noOfConnection)
    }
  
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc        = new SparkContext("local[*]","customer data")
    val input   = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning Material/data_set/friendsdata.csv")     // creating base RDD by loading the file
    //dataset : row_id :: name of the person :: age :: count of Linkedin connectoin
    //Program - To find out average number of connection of each age... 
    
    
    //val mappedInput = input.map( x => (x.split("::")(2), x.split("::")(3) )) //instead of calling anonymous function inside map we are calling named function as below
    
    val mappedInput = input.map(parseLine) 
     
    //val mappedFinalInput = mappedInput.map(  x => (x._1 , (x._2,1) ))
    //mapValue directly works on value part....
    val mappedFinalInput = mappedInput.mapValues(  x => (x,1) )  
    
    val mFinalInput = mappedFinalInput.
    reduceByKey( ( x, y ) =>  (x._1 + y._1, x._2 + y._2  ))
    
    
   val m1FinalInput = mFinalInput.
   map( x => (x._1, x._2._1/x._2._2 )).sortBy( x => x._2, false)
   
   
    mappedFinalInput.collect.foreach(println)    
}