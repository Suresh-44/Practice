import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TopNthHighest extends Serializable {
  def f(n: Int) = {
    n
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName(("NthHighest")).master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val salaryDF = Seq(("suri", 100000),
      ("s1", 110000),
      ("s2", 120000),
      ("s3", 130000),
      ("s4", 140000),
      ("s5", 150000),
      ("s6", 160000),
      ("s7", 170000),
      ("s8", 180000),
      ("s9", 190000),
      ("s10", 1100000),
      ("s11", 1110000),
      ("s12", 1120000),
      ("s13", 1130000),
      ("s14", 1140000),
      ("s15", 1150000),
      ("s16", 1160000),
      ("s17", 1170000),
      ("s18", 1180000),
      ("s19", 1190000),
      ("s20", 1200000)
    ).toDF("name", "salary")

    salaryDF.show()
    salaryDF.printSchema()

    salaryDF.createOrReplaceTempView("employee")

    val N = 9

    spark.udf.register("nvalue", f(_: Int): Int)

    spark.sql(
      """
        | select salary from (select distinct salary as salary,dense_rank() over(order by salary desc) as rank from employee) x
        | where x.rank = 9
        |""".stripMargin).show()


  }
}