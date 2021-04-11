
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
object test221 extends App{
/*  val spark = SparkSession.builder().appName("t1").master("local[*]").getOrCreate()
  import spark.implicits._
  val df = Seq(("e1","e2","e3","e4"),("vivek",null,null,"jack"),(397,null,545,448))
    .toDF("Id","Name","Sales")

  df.show()
*/
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  val sch = StructType(List(
    StructField("Id",StringType),
    StructField("Name",StringType),
    StructField("Sales",DoubleType)
  ))

  val df = spark.read
    .format("csv")
    .schema(sch)
    .option("path","C:\\Users\\suresh\\Desktop\\JavaPrograms\\src\\Spark1\\src\\main\\scala\\nullstest")
    .load()
  import spark.implicits._
  //df.show()
  val df1 = df.select(col("Id"),col("Name"),when(col("Name").isNull,col("Id"))
  .otherwise(col("Name")).alias("NameFilled"),col("Sales"))

  //df1.show()

  val df2 = df.na.fill("Nmae Missing",Seq("Name"))
      .na.fill(0)
  //df2.show()

  val df3 = df.na.fill(0.0,Array("Sales"))
    .na.fill("misssing",Array("Name"))

 // df3.show()

  val sch1 = StructType(List(
    StructField("name",StringType),
    StructField("country",StringType),
    StructField("zipcode",IntegerType)
  ))

  val df4 = spark.read
    .format("csv")
    .option("header",true)
    .schema(sch1)
    .option("path","C:\\Users\\suresh\\Desktop\\JavaPrograms\\src\\Spark1\\src\\main\\scala\\people")
    .load()

  df4.show()
  df4.printSchema()

  


  //df.na.drop("")
 // val df1 = Seq(1,2,3).toDF("numbers")
  //df1.show()






}
