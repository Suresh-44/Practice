import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import java.io.File

object SparkHiveApplication extends App{
  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Hive")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  //import spark.sql
  spark.sql("show databases").show()



}
