import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Test extends App {
 /* Logger.getLogger("org").setLevel(Level.ERROR)

  def ageCheck(age:Int):String={
    if (age>18) "Y"
    else "N"
  }

  val spark = SparkSession.builder()
    .appName("My Application 1")
    .master("local[*]")
    .getOrCreate()

  val orderSchema = StructType(List(
    StructField("order_id",IntegerType),
    StructField("order_date",TimestampType),
    StructField("order_customer_id",IntegerType),
    StructField("order_status",StringType)
  ))

  val os = "order_id Int, order_date Timestamp,order_customer_id Int,order_status String"

  val ordersDf = spark.read
      .format("csv")
    .option("header","true")
    .schema(orderSchema)
    .option("mode","DROPMALFORMED")
    .option("path","/users/suresh/Desktop/Spark/datasets/orders.csv")
    .load

  val z1 = ordersDf.na.drop("any")


  val z2 = z1.selectExpr("order_id","order_date","concat(order_status,'_STATUS') as xx ")


  //z2.show()

  import spark.implicits._
  val parseAge = udf(ageCheck(_:Int):String)

  val df2 = z1.withColumn("adult",parseAge(col("age")))
  df2.show()

  //val z3 = z1.withColumn()

  // ordersDf.show()
  // ordersDf.printSchema()

  //  case class Orders(order_id:Int,order_date:Timestamp,order_customer_id:Int,order_status:String)

  // val orderDS = z1.as[Orders]




//  val x11 = orderDS.filter(x=>x.order_id<10)

  //println(x11.rdd.getNumPartitions)
  //println(ordersDf.rdd.getNumPartitions)
  //println(spark.sparkContext.defaultMinPartitions)
  //spark.d
  //println()

//  val orderRep = ordersDf.repartition(4)
  //println(orderRep.rdd.getNumPartitions)


  //orderDS.createOrReplaceTempView("orders")

//  val red = spark.sql("select order_status FROM orders")
  //red.show()


  

  //x11.show()

  // val x1 = x.na.drop("any")

 // spark.sql("create database if not exists retail")

    z2.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(4,"order_customer_id")
    .sortBy("order_customer_id")
    .saveAsTable("retail.orders2")

  spark.catalog.listTables("retail").show()


 // val x1 = ordersDf.filter("order_id<10").show() */







}
