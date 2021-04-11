import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordCount11111")

  val ssc = new StreamingContext(sc, Seconds(20))
  val lines = ssc.socketTextStream("localhost", 9997)

  val words = lines.flatMap(_.split(" "))
  val wordPair = words.map(word=>(word,1))

  val output = wordPair.reduceByKey(_+_)
  output.print()
  ssc.start()
  ssc.awaitTermination()






































}
