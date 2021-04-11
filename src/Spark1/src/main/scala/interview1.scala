import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object interview1 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val df1 = Seq((1,"abc"),(2,"cde"),(3,"ccc"),(4,"ddd")).toDF("id","value")
  df1.show()

  val df2 = Seq((1,"ccc"),(2,"ddd"),(3,"hhh"),(4,"ddd")).toDF("id","value")
  df2.show()

  df1.createOrReplaceTempView("x")
  df2.createOrReplaceTempView("y")
  spark.sql("select x.id,y.value from x left join y where x.id = y.id").show()









}
