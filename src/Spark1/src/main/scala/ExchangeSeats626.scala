import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ExchangeSeats626 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ExchangeSeats626.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("es")

  spark.sql(
    """select a.id,b.student from es a, es b where a.id+1 = b.id
      |
      |""".stripMargin).show()




















}
