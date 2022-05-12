import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreaming1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName(("ss1")).master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df1 = spark.readStream
        .format("socket")
      .option("host","localhost")
      .option("port","12345")
      .load()

    df1.printSchema()

    df1.createOrReplaceTempView("t")
    val df2 = spark.sql("select t1.word,count() from (select explode(split(value,' ')) as word from t) t1 group by t1.word")
   // val df3 = df1.groupBy()

  val df4  = df2.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation","ck1")
    .trigger(Trigger.Once())
    .start()

    df4.awaitTermination()












  }

}
