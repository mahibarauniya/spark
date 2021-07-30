
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Program3_8_caseClass extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "program for top 10 words") 
 /*
   * Case classes define a factory method for creating instances:
   *   - lines 24-25 don't have the "new" keyword.
   *
   * Case classes generate implementations of toString, hashCode and quals:
   *   - see output in lines 24-27
   * 
   * Much more differences, like immutability and generation of companion object.
  */
  
  
  // case class
  case class StudentCase(name: String, age: Int)

  // class
  class Student(name: String, age: Int)



  var student1 = new Student("test", 10) 
  var student2 = new Student("test", 10) 
  
  // hash codes are different
  println(student1.hashCode)
  println(student2.hashCode)
  
  // so result is false
  println(student1 == student2)

  var studentCase1 = StudentCase("test", 10) 
  var studentCase2 = StudentCase("test", 10) 
  
  // hash codes are the same
  println(studentCase1.hashCode)
  println(studentCase2.hashCode)
  // so result is true
  println(studentCase1 == studentCase2)


}