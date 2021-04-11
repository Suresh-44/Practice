import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ClassesMoreThan5Students596 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("cons")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ClassesMoreThan5Students596.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("table")

  val df2 = spark.sql(
    """select class from table group by class having count(*)>=5
      |
      |""".stripMargin)

  df2.show()

  val df3 = df1.select("class")
      .groupBy("class")
      .count().as("count")
      .where(col("count")>=5)

  df3.show()


  //df2.show()















}
