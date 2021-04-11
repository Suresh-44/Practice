import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Salesperson607 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("sales")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\Salesperson607.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\company607.txt")
    .load()

  df2.show()

  val df3 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\orders607.txt")
    .load()

  df3.show()

  df1.createOrReplaceTempView("sales")
  df2.createOrReplaceTempView("company")
  df3.createOrReplaceTempView("orders")

  val df4 = spark.sql(
    """select o.sales_id,collect_list(o.com_id) as com_id from sales s join orders o on s.sales_id = o.sales_id group by o.sales_id
      |
      |""".stripMargin)

  df4.createOrReplaceTempView("orders1")

  val df5 = spark.sql(
    """select sales_id,explode(com_id) from orders1
      |
      |""".stripMargin)

  df5.show()
  df5.createOrReplaceTempView("o2")

 val df6 =  spark.sql("""select name from sales where sales_id not in (select s.sales_id from sales s join orders o on s.sales_id = o.sales_id)""")

 val df7 =  spark.sql(
    """
      |select name from sales where sales_id in (select sales_id from o2 where sales_id not in (select o2.sales_id from o2 right outer join company c on
      |o2.col = c.com_id where c.name = 'RED'))
      |""".stripMargin)

  val df8 = df6.union(df7)
  df8.show()

































}
