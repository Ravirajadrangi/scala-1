package streaming2Hive

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import tools.processStreaming


import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/*
增量的向Hive中写入Spark streaming接收到的数据
* */
object streaming2HiveIncrement {

  def main(args: Array[String]) {

    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("kafka2Streaming2Hive")
    conf.set("spark.streaming.receiver.maxRate", "200")    //200条每秒
    val ssc = new StreamingContext(conf, Seconds(60))
    val sqlContext = new HiveContext(ssc.sparkContext)
    val data = sqlContext.sql("SELECT * FROM graphx.streaming2hive")

    val zkQuorum = "10.1.80.60:2181,10.1.80.61:2181,10.1.80.65:2181,10.1.80.171:2181,10.1.80.177:2181"
    val groupId = "1"
    val topics = Map("mobile1" -> 3)
    val lines = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics).map(_._2)

    val decoded = lines.map(line => processStreaming.getWantedPage(line)).filter(_ != None).map(_.get)

    val myschema = StructType(Array(StructField("pk_mobile", StringType, true), StructField("date", StringType, true)))

    processStreaming.toHive(decoded, myschema, "graphx.streaming2hive")

    /*将Streaming的数据写入HDFS*/
//    processStreaming.toHdfs(decoded, "/data/mllib/tmp/")

    ssc.start()
    ssc.awaitTermination()
  }
}
