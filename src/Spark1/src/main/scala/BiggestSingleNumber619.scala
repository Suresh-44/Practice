import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BiggestSingleNumber619 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("sales")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\BiggestSingleNumber619.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("bsn")

  spark.sql(
    """select max(num) from bsn where num in (select num from bsn
      |group by num having count(*)=1)
      |""".stripMargin).show()











}
