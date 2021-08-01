 


 

object Program39_optionType extends App {
  
 def getAString(num: Int) : Option[String] = {
    if (num >=0) Some("A Positive Number!!")
    else None
		  
  }

 def printResult(num: Int) ={
   getAString (num) match {
     case Some(str) => println(str)
     case None => println("Not a valid number....")
   }
 }
 
printResult(10)  //result: A Positive Number!!
printResult(-10) //result: Not a valid number....


}