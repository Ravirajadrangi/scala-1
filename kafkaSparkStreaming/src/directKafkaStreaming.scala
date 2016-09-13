

import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

object directKafkaStreaming {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafkaStreaming")
    val ssc = new StreamingContext(conf, Seconds(10))


    //direct Approach(No Receivers)
    val kafkaConfig = Map("metadata.broker.list" ->"10.1.80.60:9092,10.1.80.68:9092")
    val topics = Set("mobile1")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConfig, topics)
    stream.map(_._2).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
