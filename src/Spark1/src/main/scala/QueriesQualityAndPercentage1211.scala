import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object QueriesQualityAndPercentage1211 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\QueriesQualityAndPercentage1211.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("qq")

  spark.sql(
    """select distinct(p.query_name),round((p.x/p.y),2) as quality, round(q.x1*100/p.y) as poor_query_percentage  from (select query_name,count(query_name) as y ,round(sum(rating/position)) as x from qq group by query_name) p,
      | (select count(query_name) as x1 from qq where rating<3 group by query_name) q
      |""".stripMargin).show()

  /*spark.sql(
    """ select count(query_name) as x1 from qq where rating<3 group by query_name) p
      |""".stripMargin).show() */























}
