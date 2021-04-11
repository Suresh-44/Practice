import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ConsecutiveAvailableSeats603 extends Serializable {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName(("cons")).master("local[*]")
      .getOrCreate()

    /*  def f(x:Int):Option[Int] = {
   // var x11 = 0
    var i =1


    }) */

    val df1 = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("path", "C:\\Users\\suresh\\Desktop\\Spark\\datasets\\sql datasets\\ConsecutiveAvailableSeats603.txt")
      .load()

    df1.show()



    df1.createOrReplaceTempView("tab")



    val x = df1.rdd
    val x1 = x.map(_ (1).asInstanceOf[Int])

    var i = 1
    val x2 = x1.map(x3 => {
      if (x3 == 0) {
        val y = (i,"incorrect")
        i+=1
        y
      }
      else {

        val y = (i,"correct")
        i+=1
        y

      }


    })
    import spark.implicits._
    val df3 = x2.toDF()
    df3.show()

  //  val df2 = df1.withColumn("xxxx")
   // df2.show()

  //  x2.collect().foreach(println)

    // val x2 = x1.map(xx=>xx)
    // x1.collect().foreach(println)
    // val x2 = x1.map(x1111 => x1111(0).toInt)
    //  x2.collect().foreach(println)

    // x1.collect().foreach(println)
    //x.collect().foreach(println)


  }

}
