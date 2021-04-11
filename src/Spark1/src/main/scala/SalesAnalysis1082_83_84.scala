import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SalesAnalysis1082_83_84 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\SalesAnalysis1082_83_84Products.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\SalesAnalysis1082_83_84Sales.txt")
    .load()

  df2.show()

  df1.createOrReplaceTempView("products")
  df2.createOrReplaceTempView("sales")

  spark.sql(
    """(select seller_id as price from sales group by seller_id having sum(price) = (select sum(price) from  sales
      |group by seller_id limit 1))
      |""".stripMargin).show()


  spark.sql(
    """select s.buyer_id from products p join sales s
      |on p.product_id = s.product_id where p.product_name = 'S8' and s.product_id not in (select product_id from products where product_name = 'iPhone')
      |""".stripMargin).show()

  spark.sql(
    """select p.product_id,p.product_name from products p join sales s
      |on p.product_id = s.product_id where s.sale_date between '2019-01-01' and '2019-03-31'
      |""".stripMargin).show()


















}
