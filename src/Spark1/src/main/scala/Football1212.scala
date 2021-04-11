import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Football1212 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\Football1212Teams.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\Football1212Matches.txt")
    .load()

  df2.show()

  df1.createOrReplaceTempView("teams")
  df2.createOrReplaceTempView("matches")

  val df3 = spark.sql(
    """(select t.team_id,t.team_name,s.points from (select team, sum(points) as points from (select host_team as team, host_points as points from (select host_team, guest_team,case
      | when host_goals>guest_goals then 3
      | when host_goals<guest_goals then 0
      | when host_goals=guest_goals then 1 end as host_points,
      |case
      | when host_goals>guest_goals then 0
      | when host_goals<guest_goals then 3
      | when host_goals=guest_goals then 1 end as guest_points
      | from matches) p union all
      | select q.guest_team as team, q.guest_points as points from (select host_team, guest_team,case
      | when host_goals>guest_goals then 3
      | when host_goals<guest_goals then 0
      | when host_goals=guest_goals then 1 end as host_points,
      |case
      | when host_goals>guest_goals then 0
      | when host_goals<guest_goals then 3
      | when host_goals=guest_goals then 1 end as guest_points
      | from matches) q) r group by team) s join teams t on
      | t.team_id = s.team)
      |""".stripMargin)
  df3.show()

  df3.createOrReplaceTempView("temp")

  val df4 = spark.sql("select team_id,team_name,0 as points from teams where team_id not in (select team_id from temp) ")

  val df5 = df3.union(df4).orderBy(col("team_id"))
  df5.show()

  df5.createOrReplaceTempView("ttt")

 spark.sql("select * from ttt order by points desc , team_name asc").show()

































}
