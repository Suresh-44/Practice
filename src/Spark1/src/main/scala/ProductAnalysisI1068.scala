import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ProductAnalysisI1068 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ProductSalesAnalysisI1068Sales.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ProductSalesAnalysisI1068Products.txt")
    .load()

  df2.show()


  df1.createOrReplaceTempView("sales")
  df2.createOrReplaceTempView("products")

  spark.sql(
    """select p.product_name,s.year,s.price from sales s join products p
      |on s.product_id = p.product_id
      |""".stripMargin).show()

  spark.sql(
    """select s.product_id,sum(s.quantity) from sales s join products p
      |on s.product_id = p.product_id group by s.product_id
      |""".stripMargin).show()

  spark.sql(
    """select product_id,year as first_year,quantity, price from sales where
      | (product_id,year) in (select product_id,min(year) from sales group by product_id)
      |""".stripMargin).show()


























}
