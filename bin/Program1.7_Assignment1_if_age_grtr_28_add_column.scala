import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger



object Program7_Assignment1_if_age_grtr_28_add_column extends App{
  
  def checkAdjultAge(age : Int ) ={
  if (age >18)  "Y" else "N"
  }  

  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "program for top 10 words") 
  
  val inputFile = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning Material/data_set/HCL_dataset.dat")
  inputFile.collect.foreach(println)
  
  
  val adultAge = inputFile.map( x =>  x.split(",")(1).toInt )
  val indAdult = adultAge.map(checkAdjultAge)
  indAdult.collect.foreach(println)
  
  println("Final result is as below....")  
  val result = inputFile.map( x=> (x, checkAdjultAge(x.split(",")(1).toInt)))
  result.collect.foreach(println)

}