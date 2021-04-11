import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReportContiguousDates1225 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ReportContiguousDates1225Success.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ReportContiguousDates1225Fail.txt")
    .load()

  df2.show()

  df1.createOrReplaceTempView("scs")
  df2.createOrReplaceTempView("fail")

  val df3 = spark.sql(
    """(select success_date as date ,"success" as type from scs
      | where success_date between '2019-01-01' and '2019-12-31')
      |""".stripMargin)

  val df4 = spark.sql(
    """select fail_date as date, "fail" as type from fail
      | where fail_date between '2019-01-01' and '2019-12-31'
      |""".stripMargin)

  val df5 = df3.unionAll(df4).orderBy("date")
  df5.show()

  val rdd1 = df5.select("date").rdd
  val rdd2 = rdd1.map(x=>x.toString())

  val a = rdd2.collect()

 // println(a)
  val b = df5.select("type").rdd.map(x=>x.toString()).collect()

  b.foreach(println)




















}
