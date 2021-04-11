import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._


object WindowStreamingExample extends Serializable {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "wordCount")
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost", 9992)

    /*  val wordcounts = lines.flatMap(line=>line.split(" "))
    .map(word=>(word,1))
    .reduceByKeyAndWindow((x,y)=>x+y,(x,y)=>x-y,Seconds(10),Seconds(2))
    .filter(x=>x._2>0) */

    def summary(x: String, y: String) = {
      (x.toInt * y.toInt).toString
    }

    def inversef(x: String, y: String) = {
      (x.toInt - y.toInt).toString
    }

    val wordcounts = lines.reduceByWindow(summary, inversef, Seconds(10), Seconds(2))

    val x = lines.countByWindow(Seconds(10), Seconds(2))
    //x.print()
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
