
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Program13_PremiumCustomerSalaryDoubled extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc        = new SparkContext("local[*]","customer data")
  val input   = sc.textFile("C:/Users/Barauniya/Desktop/BIG DATA/Week 9- Scala/Learning Material/data_set/customerorders.csv")     // creating base RDD by loading the file
  
  //Program to find out top 10 customers who spent maximum amount (customer, order, purchase amount...
  //only create a RDD having customer id and purchase amount and we can ignore order id .... same # of input row ...same # of output row... hence map transformation...........
  
  //val custamountData = input.map(x => (   x.split(",")(0), x.split(",")(2)  ) )  we have to convert amount to float otherwise it'll take as String input and all values will get appended with .
  val custamountData = input.map(x => (   x.split(",")(0), x.split(",")(2).toFloat  ) )
  val customerTotalAmount = custamountData.reduceByKey((a,b) => (a+b))   //reductByKey transformation for summing the salary of customer...
  val premiumCustomer = customerTotalAmount.filter( x => x._2 > 5000) 
  premiumCustomer.collect.foreach(println)
  
  println(premiumCustomer.count)  
  scala.io.StdIn.readLine() 
 
}