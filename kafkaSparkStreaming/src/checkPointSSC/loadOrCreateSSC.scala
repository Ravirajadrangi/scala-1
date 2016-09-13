package checkPointSSC

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by dyh.
  * 如果checkPoint的路径上保存有StreamingContext则加载，否则新建
  */
object loadOrCreateSSC {
    def createStreamingContext(): StreamingContext = {
      val conf = new SparkConf().setMaster("local[2]").setAppName("posDirectKafkaStreaming")
      val ssc = new StreamingContext(conf, Seconds(60))
      ssc.checkpoint("/data/mllib/streaming")
      ssc
    }
}
