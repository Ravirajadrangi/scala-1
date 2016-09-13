
import org.apache.spark._
import org.apache.spark.streaming._

object testStreaming {
    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local[2]").setAppName("network")
        val ssc = new StreamingContext(conf, Seconds(1))

        val lines = ssc.socketTextStream("localhost", 9998)

        val words = lines.flatMap(_.split(" "))

        val paris = words.map(word => (word, 1))
        val wordCounts = paris.reduceByKey(_ + _)

        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
