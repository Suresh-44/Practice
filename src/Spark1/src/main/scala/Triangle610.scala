import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Triangle610 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("sales")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\Triangle610.txt")
    .load()

  df1.createOrReplaceTempView("triangle")

  spark.sql(
    """select x,y,z, case
      | when x+y>z and x+z>y and y+z>x then "YES"
      | else "NO"
      | end as Triangle
      | from triangle
      |""".stripMargin).show()




























}
