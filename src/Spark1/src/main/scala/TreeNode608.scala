import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TreeNode608 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("sales")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\TreeNode608.txt")
    .load()

  df1.show()


  df1.createOrReplaceTempView("tree")

  spark.sql(
    """select
      |id,
      |case(p_id)
      |when  null then 'Root'
      |when not null and id in (select distinct p_id from tree) then 'Inner'
      |else 'Leaf' end as Type
      |from tree
      |""".stripMargin).show()

  import spark.implicits._























}
