package realTimeSimulation.test.checkpoint


import tools.processStreaming.toES
import checkPointSSC.loadOrCreateSSC
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by yn on 2016/7/15.
  */
object testCheckPonit {
  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: Spark-submit --configs <topic>") //pos1
      sys.exit(1)
    }
    val Array(topic, _*) = args

    //.setMaster("local[2]")
//    LogManager.getRootLogger.setLevel(Level.ERROR)
    val ssc = StreamingContext.getOrCreate("/data/mllib/streaming", ()=>createStreamingContext(topic))


    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext(topic:String): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("posDirectKafkaStreaming")
    val ssc = new StreamingContext(conf, Seconds(60))
    ssc.checkpoint("/data/mllib/streaming")

    val kafkaConfig = Map("metadata.broker.list" -> "10.1.80.60:9092,10.1.80.68:9092")
    val topics = Set(topic)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConfig, topics)

    val lines: DStream[Row] = stream.map(_._2).map(Row(_))

    val schema = StructType(Array(StructField("pos", StringType, true)))

    toES(lines, schema, "pos/pos_checkpoint", "10.1.80.75")

    ssc
  }
}
