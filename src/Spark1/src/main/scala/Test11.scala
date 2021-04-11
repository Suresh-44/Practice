import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.Encoders



object Test11 extends App {
  case class c1(_c0:String,_c1:String,_c2:String)
  case class address1(pin:Long,street:String)
  case class ss(address:address1,age:Long,name:String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val schema = ScalaReflection.schemaFor[ss].dataType.asInstanceOf[StructType]
  val encoderSchema = Encoders.product[ss].schema
  encoderSchema.printTreeString()

  val schema1 = StructType(List(
    StructField("_c0",StringType),
    StructField("_c1",StringType),
    StructField("_c2",StringType)
  ))

  val schema2 = StructType(List(

    StructField("address",StructType(List(
        StructField("pin",LongType),
        StructField("street",StringType)
      ))),
    StructField("age",LongType),
    StructField("name",StringType)
  ))

  val spark = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read
      .format("json")
    .option("path","C:\\Users\\suresh\\Desktop\\Spark\\datasets\\StructType.txt")
    .load()

  //df.printSchema()
  import spark.implicits._

  val r1 = df.rdd
 //r1.collect().foreach(println)

  val ds = df.as[ss]
 // ds.show(false)
  ds.printSchema()

  //val r2 = ds.rdd
 // r2.collect().foreach(println)

 // val df2 = r2.toDF()
  //df2.show()

//  df.selectExpr("address.pin").show()

// val d2 = spark.sqlContext.createDataFrame(r1,schema2)

 //d2.show()


 // val d2 = r1.toDF()

 //val r2 = r1.map(x=>x(0))
  //r2.collect().foreach(println)

  //df.show(false)

  //df.selectExpr("address").show(false)




}
