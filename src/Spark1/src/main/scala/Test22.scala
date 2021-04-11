import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object Test22 extends App{
  case class Student(name:String,id:Int,age:Int,course:String,gender:String)
  val s1 = new Student("suri",1,27,"ANN","M")
  val s2 = new Student("suresh",2,26,"ML","M")
  val s3= new Student("suresh",2,28,"ML","M")
  val s4 = new Student("suresh",2,25,"ML","M")
  val s5 = new Student("suresh",2,29,"ML","M")
  val s6 = new Student("suresh",2,24,"ML","M")
  val s7 = new Student("suresh",2,30,"ML","M")
  val s8 = new Student("suresh",2,23,"ML","M")

  case class c1(id:Long,city:String,marks:Array[Long])

  val sch = StructType(List(
    StructField("city",StringType),
    StructField("id",LongType),
    StructField("marks",ArrayType(LongType))
  ))

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read
    .format("json")
    .option("path","C:\\Users\\suresh\\Desktop\\Spark\\datasets\\jsonArray.txt")
    .load()

//  df.show(false)

  //df.printSchema()

  val rdd = df.rdd
  //rdd.collect().foreach(println)

  val df1 = spark.sqlContext.createDataFrame(rdd,sch)

 // df1.show()

  import spark.implicits._
  val ds = df.as[c1]

  val a = spark.createDataFrame(Array((1,2),(2,3),(4,5)))
  //a.show()

  val b= Seq((1,2),(3,5),(6,7))
  val df3 = b.toDF()
  //df3.show()

  val ds11 = spark.createDataset(Array((1,"z"),(2,"f"),(3,"6")))

  //ds11.show()


  val x11 = spark.createDataFrame(Array(s1,s2,s3,s4))
  x11.show()
  val x22 = spark.createDataFrame(Array(s5,s6,s7,s8))
  x22.show()

  val sjoin = x11.join(x22,x11("age")===x22("age"),"inner")
  sjoin.show()
  //x11.map(x=>(x(0),x(1))).collect().foreach(println)

 // val x12 = spark.createDataset(Array(s1,s2))

  //x12.show()

 // x12.map(x=>(x.name,x.age)).show()

  //val x22 = x11.selectExpr("name","age")
  //x22.show()
  //x22.collect().foreach(println)
 // x22.collect().foreach(println)


  //x11.show()

 // ds.show()
  //ds.printSchema()

  // df.printSchema()

 // spark.sqlContext.sql("show functions").show()



































}
