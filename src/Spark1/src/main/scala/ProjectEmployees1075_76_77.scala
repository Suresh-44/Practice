import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ProjectEmployees1075_76_77 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ProjectEmployees1075_76_77ProjectTable.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ProjectEmployees1075_76_77EmployeeTable.txt")
    .load()

  df2.show()

  df1.createOrReplaceTempView("project")
  df2.createOrReplaceTempView(("employee"))

  spark.sql(
    """select p.project_id,avg(e.experience_years) from project p join employee e
      |on p.employee_id = e.employee_id group by p.project_id
      |""".stripMargin).show()

  spark.sql(
    """select project_id from (select project_id,count(employee_id) as cnt from project group by project_id) new
      | limit 1
      |""".stripMargin).show()
























}
