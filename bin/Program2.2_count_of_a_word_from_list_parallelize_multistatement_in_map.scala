import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Program11_count_of_a_word_from_list  extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "program for counting the word from rdd where data is coming from list")
    var myList = List ("WARN: Tuesday 17 June 2021", 
                                 "ERROR: Saturday 09 Jan 2011", 
                                 "ERROR: Sunday 07 May 2013", 
                                  "ERROR: Friday 11 Jan 2017", 
                                  "ERROR: Saturday 09 Jan 2011")
     
      // create rdd from list or data available in local.
      val origRdd   = sc.parallelize(myList)
      val origRdd1 =  origRdd.map( x => 
        {
          val column1 = x.split(":")
          val logLevel  = column1(0)
          (logLevel,1)
        }
      )
      val resultRdd = origRdd1.reduceByKey((x,y) => x+y )
     resultRdd.collect.foreach(println)
}