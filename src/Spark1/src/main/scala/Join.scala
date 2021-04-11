import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Join  extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("joins")
    .master("local[*]")
    .getOrCreate()

  val ordersdf = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("path","/users/suresh/Desktop/Spark/datasets/on")
    .load()

  val customersdf = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("path","/users/suresh/Desktop/Spark/datasets/cs")
    .load()

 // ordersdf.show()
 // customersdf.show()

 // val joinCondition = ordersdf.col("order_customer_id")===customersdf.col("customer_id")
  //val joinType = "inner"
  //val innerdf = ordersdf.join(customersdf,joinCondition,joinType).sort("order_customer_id")

  //innerdf.show()

  val joinCondition1 = ordersdf.col("order_customer_id")===customersdf.col("customer_id")
  val joinType1 = "outer"
  val innerdf1 = ordersdf.join(customersdf,joinCondition1,joinType1).sort("order_customer_id")

  innerdf1.show()


  //scala.io.StdIn.readLine()
  //spark.stop()
















































}
