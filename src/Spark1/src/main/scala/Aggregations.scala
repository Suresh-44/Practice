
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window


object Aggregations extends App {

  Logger.getLogger("org").setLevel(Level.ERROR) 

  val windowSchema = StructType(List(
    StructField("Country",StringType),
    StructField("weeknum",IntegerType),
    StructField("x",IntegerType),
    StructField("Y",IntegerType),
    StructField("InvoiceValue",FloatType)
  ))

  val spark  = SparkSession.builder()
    .appName("app3")
    .master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("path","/users/suresh/Desktop/Spark/datasets/order_data.csv")
    .load

  val dfw = spark.read
    .format("csv")
    .schema(windowSchema)
    .option("header","true")
    .option("inferSchema","true")
    .option("path","/users/suresh/Desktop/Spark/datasets/windowdata.csv")
    .load

  dfw.show()

  //df1.show()

  println(df1.count())

  ///////////////////   *****   SIMPLE AGGREGATIONS  *********//////////////////////////

  // COLUMN OBJECT EXPRESSION  ///////////

  val df2 = df1.select(
    count("*").as("RowCount"),
    sum(col("Quantity")).as("TotalQuantity"),
    avg(col("UnitPrice")).as("AvgUnitPrice"),
    countDistinct(col("InvoiceNo")).as("UniqueInvoice")).show()

  // COLUMN STRING EXPRESSION ///////////

  val df3 = df1.selectExpr("count(*) as RowCount","sum(Quantity) as TotalQuantity","avg(UnitPrice) as " +
    "AvgUnitPrice","count(Distinct(InvoiceNo)) as UniqueInvoice" ).show()

  // SPARK - SQL  /////////////////////

  df1.createOrReplaceTempView("orderData")
  val df4 = spark.sql("select count(*) as RowCount, avg(UnitPrice) as AvgUnitPrice, count(distinct(InvoiceNo)) as UniqueInvoice from orderData").show()

  ///////////////////////// ******  GROUPING AGGREGATIONS  ****** /////////////////////////////

  //////  COLUMN OBJECT EXPRESSION  ////////////////////////
  val df5 = df1.groupBy("Country","InvoiceNo").agg(sum("Quantity").as("TotalQuantity"),
    sum(col("Quantity")*col("UnitPrice")).as("SumOfInvoiceValue")).show()

  ///// COLUMN STRING EXPRESSION  //////////////////////////

  val df6 =  df1.groupBy("Country","InvoiceNo")
    .agg(expr("sum(Quantity) as TotalQuantity"),expr("sum(Quantity*UnitPrice) as InvoiceValue")).show()

  //////////   SPARK - SQL  ///////////////

  val df7 = spark.sql("select sum(Quantity) as TotalQuantity," +
    " sum(Quantity*UnitPrice) as InvoiceValue from orderData group by Country,InvoiceNo").show()

  //////////  WINDOW AGGREGATION  /////////////////////////

  val myW = Window.partitionBy("Country")
    .orderBy("weeknum")
    .rowsBetween(Window.unboundedPreceding,Window.currentRow)

  val myDF = dfw.withColumn("RunningTotal",sum("InvoiceValue").over(myW))
  myDF.show()

  dfw.createOrReplaceTempView("window")

  val df8 = spark.sql("select Country,weeknum,InvoiceValue,sum(InvoiceValue) over(partition by Country order by weeknum rows between unbounded preceding and current row) as RunningSum from window").show()

































}
