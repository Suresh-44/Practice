import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ShortestDistance612 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("sales")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ShortestDistance612.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("sd")

  spark.sql(
    """select min(sqrt(pow((a.x-b.x),2)+pow((a.y-b.y),2))) as shortest from sd a, sd b
      |where !(a.x=b.x and a.y=b.y)
      |""".stripMargin).show()

















}
