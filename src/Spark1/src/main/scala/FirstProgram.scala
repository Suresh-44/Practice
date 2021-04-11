import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object FirstProgram extends App{
 /*  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","word count")
  val rdd = sc.textFile(raw"C:/Users/suresh/Desktop/Spark/friends-data.csv")
  val rdd1 = rdd.map(x=>x.split("::"))
  val rdd2  = rdd1.map(x=>(x(2),(x(3).toFloat,1)))
  val rdd3 = rdd2.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val rdd4 = rdd3.map(x=>(x._1,(x._2._1/x._2._2)))
  val rdd5 = rdd4.sortByKey()
  rdd5.collect().foreach(println)

  //scala.io.StdIn.readLine()

  */


}
