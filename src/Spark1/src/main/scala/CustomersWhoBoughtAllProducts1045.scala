import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CustomersWhoBoughtAllProducts1045 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\CustomersWhoBoughtAllProducts1045customersTable.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\CustomersWhoBoughtAllProducts1045productsTable.txt")
    .load()

  df2.show()

  df1.createOrReplaceTempView("customers")
  df2.createOrReplaceTempView("products")

  spark.sql(
    """select customer_id from (select c.customer_id,c.product_key from customers c join products p
      |on c.product_key = p.product_key) c1 group by customer_id having count(distinct(product_key)) = (select count(product_key) from products)
      |""".stripMargin).show()
}
