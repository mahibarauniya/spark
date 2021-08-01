

object Program39_optionType extends App {
 /* 
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
*/
  

  case class Address ( city: String, state: String, zip: String)
  
  class User(email: String, password: String){
    var firstname : Option[String] = None
    var lastname : Option[String] = Some("Barauniya")
    var address : Option[Address] = None
  }
  
  var usr = new User ( "Mahimmmm07@gmail.com", "XYZ")
  
 println(usr.firstname.getOrElse("<Not assigned>")) // <Not assigned>
 println(usr.lastname.getOrElse("<Not assigned>"))  //Barauniya
     
}