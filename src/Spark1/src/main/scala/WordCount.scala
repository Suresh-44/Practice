import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

import scala.io.Source


object WordCount extends App {
/*
  Logger.getLogger("org").setLevel(Level.ERROR)

  def boringWords():Set[String] = {
    var boringwords:Set[String] = Set()
    val lines = Source.fromFile("/users/suresh/Desktop/Spark/datasets/boringwords.txt").getLines()

    for (line <- lines){
      boringwords+= line
    }
    boringwords
  }
  //println("aqqqqq")

  val sc = new SparkContext("local[*]","wordCount")

  val rdd1 = sc.textFile("/users/suresh/Desktop/Spark/datasets/bigdata-campaign-data.csv")

  val br = sc.broadcast(boringWords)

  val rdd2 =  rdd1.map(x=>x.split(","))

  val rdd3 = rdd2.map(x=>(x(10).toFloat,x(0)))

  val rdd4 = rdd3.flatMapValues(x=>(x.split(" ")))


  rdd4.collect().foreach(println)
  //println(x)
  //scala.io.StdIn.readLine()






*/




























}
