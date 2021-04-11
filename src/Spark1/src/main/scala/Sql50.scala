import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Sql50 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("joinsinterview")).master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val workerdf = Seq((1, "Monika", "Arora", 100000, "14-02-20 09.00.00", "HR"),
  (2, "Niharika", "Verma", 80000, "14-06-11 09.00.00", "Admin"),
  (3, "Vishal", "Singhal", 300000, "14-02-20 09.00.00", "HR"),
  (4, "Amitabh", "Singh", 500000, "14-02-20 09.00.00", "Admin"),
  (5, "Vivek", "Bhati", 500000, "14-06-11 09.00.00", "Admin"),
  (6, "Vipul", "Diwan", 200000, "14-06-11 09.00.00", "Account"),
  (7, "Satish", "Kumar", 75000, "14-01-20 09.00.00", "Account"),
  (8, "Geetika", "Chauhan", 90000, "14-04-11 09.00.00", "Admin"))
    .toDF("WORKER_ID", "FIRST_NAME", "LAST_NAME", "SALARY", "JOINING_DATE", "DEPARTMENT")

  workerdf.show()
  workerdf.createOrReplaceTempView("worker")

  val bonusdf = Seq((1, 5000, "16-02-20"),
  (2, 3000, "16-06-11"),
  (3, 4000, "16-02-20"),
  (1, 4500, "16-02-20"),
  (2, 3500, "16-06-11")).toDF("WORKER_REF_ID", "BONUS_AMOUNT", "BONUS_DATE")

  bonusdf.show()

  bonusdf.createOrReplaceTempView("bonus")

  val titledf = Seq((1, "Manager", "2016-02-20 00:00:00"),
  (2, "Executive", "2016-06-11 00:00:00"),
  (8, "Executive", "2016-06-11 00:00:00"),
  (5, "Manager", "2016-06-11 00:00:00"),
  (4, "Asst. Manager", "2016-06-11 00:00:00"),
  (7, "Executive", "2016-06-11 00:00:00"),
  (6, "Lead", "2016-06-11 00:00:00"),
  (3, "Lead", "2016-06-11 00:00:00"))
    .toDF("WORKER_REF_ID", "WORKER_TITLE", "AFFECTED_FROM")

  titledf.show()

  titledf.createOrReplaceTempView("title")

  val n = 5

  import scala.collection.mutable.ListBuffer

  var a = new ListBuffer[Int]()

  def f1(n:Int):ListBuffer[Int] ={
    var i = 0
    while (i<n){
      a +=1
      i+=1
    }
    a
  }

  println(f1(n))

  






}
