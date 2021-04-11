import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object NestedData extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("nesteddata")).master("local[*]")
    .getOrCreate()

  import spark.implicits._

/*  val df = Seq("""{
    "student_name":"Ram kumar",
    "DOB" : "10–02–1995"
    "address":{"city":"kolkata","pin_code":811320},
    "contact_number":{"primary":963343944,"secondary":883673363 }
  }""").toDF() */

  val df = spark.read
      .option("multiLine","true")
      .format("json")
    .option("path","nestedjson1")
      .load()

  //df.show(false)
  //df.printSchema()
  val x = df.schema
  //x.printTreeString()

  //df.rdd.isEmpty()

val z =   (a:Int)=>a*2
  println(z(2))




}
