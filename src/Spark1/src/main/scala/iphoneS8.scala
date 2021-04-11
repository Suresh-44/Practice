import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object iphoneS8 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("iphone")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\iphoneS8ProductsTable.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\iphoneS8SalesTable.txt")
    .load()

  df2.show()

  df1.createOrReplaceTempView("products")
  df2.createOrReplaceTempView("sales")

  /*val df3 = spark.sql(
    """select s.buyer_id from products p join sales s on
      |p.product_id=s.product_id where p.product_name =  'iphone'
      |""".stripMargin).show() */

  val df4 = spark.sql(
    """ select * from (select buyer_id from sales where product_id in (select product_id from products where product_name='S8')) where buyer_id not in
      | (select buyer_id from sales where product_id in (select product_id from products where product_name='iPhone')) """.stripMargin)
  df4.show()

 // spark.sql("select product_id from products where product_name='S8' ").show()

  // and product_id not in (select product_id from products where product_name = 'iPhone' ))



























}
