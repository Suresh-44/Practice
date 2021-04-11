import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PageRecommendations1264 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\PageRecommendations1264Friends.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\PageRecommendations1264Likes.txt")
    .load()

  df2.show()

  df1.createOrReplaceTempView("friends")
  df2.createOrReplaceTempView("likes")

  spark.sql(
    """select distinct(l.pageid) from likes l join  ((select user2 as x from friends where user1 = 1) union all
      |(select user1 as x from friends where user2 = 1)) P on l.userid = p.x where l.pageid not in (select pageid from likes where userid = 1)
      |""".stripMargin).show()


























}
