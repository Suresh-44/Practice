

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
object Joins1  {
  case class Logging(level:String,datetime:String)
  def mapper(line:String)={
    val fields = line.split(",")
    val output:Logging = Logging(fields(0), fields(1))

    //val output = (fields(0),fields(1))
    output
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("joins1")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

 /*   val myrecords = Seq(("WARN,2016-12-31 04:19:32"),
      ("FATAL,2016-12-31 04:19:32"),
      ("WARN,2016-12-31 04:19:32"),
      ("INFO,2016-12-31 04:19:32"))

    val myrecordsrdd = spark.sparkContext.parallelize(myrecords)

    //myrecordsrdd.collect().foreach(println)


    val myrecordsNew = myrecords.map(mapper)
    val myrecordsDF = myrecordsNew.toDF()
    val myrecordsDF1 = myrecordsDF.withColumn("datetime",col("datetime").cast(TimestampType))

    //myrecordsDF1.printSchema()
    //myrecordsDF1.show()

    myrecordsDF1.createOrReplaceTempView("ll")
    val df1 = spark.sql("select level, date_format(datetime,'MMMM') as monthname from ll")
    //df1.show()
    df1.createOrReplaceTempView("ll1")
    val df2 = spark.sql("select level,monthname , count(*) as count from ll1 group by level,monthname")
   // df2.show()
    //val options = Map("header"->"true")

    val realdf = spark.read
      .format("csv")
      .option("header","true")
      .option("path",raw"C:\\Users\\suresh\\Desktop\\Spark\\datasets\\biglog.txt")
      .load()

    //realdf.show()

    realdf.createOrReplaceTempView("ll2")
    val df3 = spark.sql("select level,date_format(datetime,'MMMM') as monthname from ll2 ")
    //df3.show()

    df3.createOrReplaceTempView("ll3")

    val df4 = spark.sql("select level,monthname,count(*) as count from ll3 group by level,monthname")
    //df4.show(100)

    //spark.sql("select add_months('2020-08-01',4) as new_month").show()

    //spark.sql("select current_date").show()

    //spark.sql("select current_timestamp").show(false)

    //spark.sql("select date_format('2020-08-21','')").show()    */

    import spark.implicits._

  /*  Seq(("2019-12-30"))
      .toDF("Input")
      .select(
        current_date() as("curDate"),
        col("Input"),
        date_format(col("Input"),"MM/dd/yyyy") as("format")
      ).show()

    Seq("2019-12-30").toDF("Input")
      .select(
        col("Input"),
        to_date(col("Input"),"yyyy-MM-dd").as("todate")).show()


    Seq(("04/13/2019"))
      .toDF("Input")
      .select( col("Input"),
        to_date(col("Input"), "MM/dd/yyyy").as("to_date")
      ).show()

    Seq(
      ("2019-12-30"),
      ("2019-12-30"),
      ("2019-12-30")
    ).toDF("Input")
      .select(
        col("Input"),
        current_date,
        datediff(current_date(),col("Input")).as("dateDifference")
      ).show()
*/
    val df = Seq(("2019-07-01 12:01:19.000"),
      ("2019-06-24 12:01:19.000"),
      ("2019-11-16 16:44:55.406"),
      ("2019-11-16 16:50:59.406")).toDF("input_timestamp")

    df.show(false)

    df.createOrReplaceTempView("x")
  //  spark.sql("select dayofweek(input_timestamp),weekofmonth(input_timestamp) from x").show()

    df.withColumn("input_timestamp",
      to_timestamp(col("input_timestamp")))
      .withColumn("week_day_number", dayofweek(col("input_timestamp")))
      .withColumn("week_day_abb", date_format(col("input_timestamp"), "E"))
      .show(false)

    df.withColumn("input_timestamp",
      to_timestamp(col("input_timestamp")))
      .withColumn("week_day_full", date_format(col("input_timestamp"), "EEEE"))
        .persist(StorageLevel.DISK_ONLY)

    println(spark.sparkContext.defaultMinPartitions)








   // df1.create






   /* val schema = StructType(List(
      StructField("Level",StringType),
      StructField("DateTime",TimestampType)
    )) */


    //val mydf = spark.createDataFrame(,schema)
    //mydf.show()
    //myrecordsNew.foreach(println)
    //val x = myrecordsNew.toSeq
   // val xx = myrecordsNew.toDF




  }


}
