import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object customersStats extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\CustomersSummary.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("customers")


  spark.sql(
    """select concat(CITY,':',count(ID)) as Result from customers group by CITY order by CITY
      |""".stripMargin).show()




}
