import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReportToManager1270 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ReportToManager1270.txt")
    .load()

  df1.show()

  df1.createOrReplaceTempView("emp")
  spark.sql(
    """select distinct(employee_id) from emp where manager_id in ((select employee_id from emp where manager_id in (select employee_id from emp where manager_id = 1 and employee_id != 1))
      |union all
      |(select employee_id from emp where manager_id = 1 and employee_id != 1)) union all
      |((select employee_id from emp where manager_id in (select employee_id from emp where manager_id = 1 and employee_id != 1))
      |union all
      |(select employee_id from emp where manager_id = 1 and employee_id != 1))
      |""".stripMargin).show()

































}
