import org.apache.spark.sql.SparkSession

object DataFrameExamples extends App{
/*
   val spark = SparkSession.builder()
    .appName("My Application 1")
    .master("local[*]")
    .getOrCreate()

  //val rd1 = spark.sparkContext.parallelize(Array((1,2,3),(2,3,4),(1,2,3)))
  //rd1.collect.foreach(println)

  val df1 = spark.read
    .format("csv")
    .option("header","true")
    .load(raw"C:\\Users\\suresh\\Desktop\\ScalaPrograms\\sample1.txt")

  df1.show
  df1.printSchema

  case class x(i:String,j:String,k:String,l:String)
  import spark.implicits._
  val ds1 = df1.as[x]

  ds1.collect.foreach(println)

  */


}
