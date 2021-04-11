import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object test222 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("test222").master("local[*]").getOrCreate()

  val df = spark.read
    .format("csv")
    .option("header",true)
    .option("path","C:\\Users\\suresh\\Desktop\\Spark\\datasets\\person.txt")
    .load()

 // df.show()

  val rdd1 = df.rdd
  // rdd1.collect().foreach(println)

  val sch = StructType(List(
    StructField("c0",StringType),
    StructField("c1",StringType),
    StructField("c2",StringType)
  ))

  val df1 = spark.sqlContext.createDataFrame(rdd1,sch)
  // df1.show()



  scala.io.StdIn.readLine()


}
