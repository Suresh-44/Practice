import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StudentsAndExaminations1280 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName(("es")).master("local[*]")
    .getOrCreate()

  val df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\StudentsAndExaminations1280Exams.txt")
    .load()

  df1.show()

  val df2 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\StudentsAndExaminations1280students.txt")
    .load()

  df2.show()

  val df3 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\StudentsAndExaminations1280subjects.txt")
    .load()

  df3.show()

  df1.createOrReplaceTempView("exams")
  df2.createOrReplaceTempView("students")
  df3.createOrReplaceTempView("subjects")

 val df4 =  spark.sql(
    """(select p.student_id,s.student_name,p.subject_name, p.attended_exams  from students s join (select student_id,subject_name,count(subject_name) as attended_exams
      | from exams group by student_id,subject_name) p
      | on s.student_id = p.student_id order by p.student_id, s.student_name)
      |""".stripMargin)

  df4.createOrReplaceTempView("temp")

  val df5 = spark.sql(
    """select student_id,student_name from students where
      | student_id not in   (select student_id from temp) """.stripMargin)

  df5.show()
  df5.createOrReplaceTempView("temp1")
  val df6 =   spark.sql(
    """select t1.student_id,t1.student_name,s.subject_name,0 as attended_exams from
      | subjects s, temp1 t1
      |""".stripMargin)

  val df7 = df4.unionAll(df6).show()

  spark.sql(
    """select e.student_id,s.student_name,s.subject_name, count(e.student_id) as att from
      |  (select s.student_id, s.student_name, u.subject_name
      | from students as s join subjects as u) s left join exams e on s.student_id = e.student_id
      | group by e.student_id,s.student_name,s.subject_name
      |""".stripMargin).show()


















}
