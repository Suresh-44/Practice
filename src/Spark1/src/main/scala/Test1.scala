import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}



object Test1  {
  case class Orders(order_id:Int,customer_id:Int,order_status:String)
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
/*   def ageCheck(age: Int): String = {
      if (age > 18) "Y"
      else "N"
    }

    val personSchema = StructType(List(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("city", StringType)
    ))

    val spark = SparkSession.builder()
      .appName("My Application 1")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.read
      .format("csv")
      .schema(personSchema)
      .option("header", "true")
      .option("path", "/users/suresh/Desktop/Spark/datasets/person.txt")
      .load

    spark.udf.register("parseAge", ageCheck(_: Int): String)
   // val df2 = df1.withColumn("adult", parseAge(col("age")))
    val df3 = df1.selectExpr("name","parseAge(age) as tzr")
    df3.show()

    import spark.implicits._

    //val df4 = Seq((1,2,3),(1,3,4),(1,2,3)).toDF("x","y","z")

    val df4 = Seq(
      ("Thor","New York"),
      ("Aquaman","Atlantis"),
      ("Wolverine","New York")
    ).toDF("Movie","City")

    //df4.show()

   // df4.withColumn("city_starts_with",col("City").startsWith("New")).show()


    //val df3 = df1.withColumn("xx",expr("concat(city,'xxxxxx')"))
   // val df3 = df1.withColumn("xx",concat(col("city"),lit("nnn")))
    //df3.show() */

    val myregex = """^(\S+)\s(\S+)\t(\S+)\,(\S+)""".r
   def parser(line:String)={
     line match {
       case myregex(order_id,date,customer_id,order_status) => Orders(order_id.toInt,customer_id.toInt,order_status)
     }
   }
   val spark = SparkSession.builder()
     .appName("My Application 2")
     .master("local[*]")
     .getOrCreate()

   val lines = spark.sparkContext.textFile("/users/suresh/Desktop/Spark/datasets/on.txt")

   //lines.collect().foreach(println)
   import spark.implicits._
   val ods = lines.map(parser)

    ods.collect().foreach(println)

    val ds = ods.toDS()

    ds.show()



 //   ods.printSchema()
   // ods.select("order_id").show()


  }
}