import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object test112 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()


  import spark.implicits._

  val df = Seq(1,2,3).toDF("numbers")
 // df.show()

  val a = Array(5,6,6,7)
  println(a)
  a.foreach(println)


  // HashMap

  val ratings = Map("Ladies in the water"->3.0,"snakes on a plane"->2.0,"you,me and dupree"->9.0)

  val ratings1 = ratings-"you,me and dupree"

  println(ratings1)

  for ((k,v)<-ratings){
    println(s"key:$k, value:$v")
  }

  val x = ratings.keys.map(x=>(x))
  println(x)


  val x111 = ratings.keys
  val x112 = x111.toList
  println(x112)

  //val x1 = ratings.mapValues(_+1)
  //println(x1)

  val x1 = ratings.map(x=>(x._1,x._2+1))
  println(x1)

  val x2 = ratings.mapValues(x=>(x+1))
  println(x2)

  val x3 = ratings.transform((x,y)=>(y+1))
  println(x3)

  var x4:Map[String,Int] = Map()
  x4+= ("a"->1)
  x4+=("b"->2)

  println(x4)

  var x5:Set[Int] = Set()
  x5+=8
  x5+=5
  x5+=9

  println(x5)

  var x6 = Seq(1,2,3,4,5,6,7,8,9)
  println(x6)


  val x7 = Seq(2,3,4,5,6,7,8,9,0,1)

  val x8 = x6 zip x7
  println(x8)


//  val x9 = x7.unzip()
 // println(x9)















}
