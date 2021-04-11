import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object NumberOfCommentsPerPost1241 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\NumberOfCommentsPerPost1241.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("post")

/* val df2 =  spark.sql(
    """select parent_id as post_id, count(distinct(sub_id)) as number_of_comments
      | from post
      | group by parent_id having parent_id in (select distinct sub_id from (select sub_id from post where parent_id = 'Null'))
      |""".stripMargin) */

 /* spark.sql(
    """select distinct(parent_id) from post where parent_id  in  (select distinct sub_id from (select sub_id from post where parent_id = 'Null'))
      |
      |""".stripMargin).show() */

  spark.sql(
    """select 0 as co
      |
      |""".stripMargin).show()

































}
