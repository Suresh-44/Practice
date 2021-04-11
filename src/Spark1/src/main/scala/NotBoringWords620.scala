import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object NotBoringWords620 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("boringwords")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\NotBoringWords620.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("nbw")

  spark.sql(
    """select * from nbw where id in (select id from nbw where id%2=1)
      | and description != 'boring' order by rating desc
      |""".stripMargin).show()
























}
