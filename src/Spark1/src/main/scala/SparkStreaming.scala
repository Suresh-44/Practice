import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._

object SparkStreaming  {

  def main(args:Array[String])= {

    Logger.getLogger("org").setLevel(Level.ERROR)
    def updatefunction(newValues:Seq[Int],previousState:Option[Int]):Option[Int]={
      val count = previousState.getOrElse(0)+newValues.sum
      Some(count)
    }

    val sc = new SparkContext("local[*]", "wordCount11111")

    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    ssc.checkpoint(".")
    val words = lines.flatMap(x => x.split(" "))
    val pairs = words.map(x => (x, 1))

    val wordCounts = pairs.updateStateByKey(updatefunction)

    wordCounts.print()


    //val x = x1.updateStateByKey(())
    // x.print()

    //val words = lines.flatMap(x=>x.split(" "))
    // val pairs = words.map(x=>(x,1))
    // val wordCounts = pairs.reduceByKey(_+_)
    //wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
