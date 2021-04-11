import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Test2 extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val orderSchema = StructType(List(
      StructField("order_id", IntegerType),
      StructField("order_date", StringType),
      StructField("order_customer_id", IntegerType),
      StructField("order_status",StringType)
    ))


    val spark = SparkSession.builder()
      .appName("My Application 2")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.read
      .format("csv")
      .schema(orderSchema)
      .option("header", "true")
      .option("mode","DROPMALFORMED")
      .option("path", "/users/suresh/Desktop/Spark/datasets/orders.csv")
      .load

    df1.show()
    df1.printSchema()



    val df2 = df1
      .withColumn("order_date",unix_timestamp(col("order_date").cast(TimestampType)))
        .withColumn("new_id",monotonically_increasing_id())
      .dropDuplicates("order_date","order_customer_id")
        .drop("order_id")
        .sort("order_date")

    df2.show()
    df2.printSchema()




}
