import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object pivot extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("xyz")).master("local[*]")
    .getOrCreate()

  import spark.implicits._
  case class Student(name:String, gender:String, weight:Int, graduation_year:Int)
  val studentsDF = Seq(Student("John", "M", 180, 2015),
    Student("Mary", "F", 110, 2015),
    Student("Derek", "M", 200, 2015),
    Student("Julie", "F", 109, 2015),
    Student("Allison", "F", 105, 2015),
    Student("kirby", "F", 115, 2016),
    Student("Jeff", "M", 195, 2016)).toDF()

  studentsDF.show()

  // calculating the average weight for each gender per graduation year

  val df1 = studentsDF.groupBy("gender","graduation_year")
    .avg("weight").show()

  val df2 = studentsDF.groupBy("graduation_year")
    .pivot("gender").avg("weight").show()

  studentsDF.groupBy("graduation_year").pivot("gender")
    .agg(
      min("weight").as("min"),
      max("weight").as("max"),
      avg("weight").as("avg")
    ).show()


  






























}
