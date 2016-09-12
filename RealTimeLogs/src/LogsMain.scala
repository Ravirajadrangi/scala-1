

import kafka.serializer.StringDecoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import lakala.spark.logs.yarn.resourceManager.JobInfo
import lakala.spark.logs.yarn.resourceManager.ContainerInfo
import lakala.spark.logs.yarn.resourceManager.JobContainerInfo

/**
  * Created by dyh on 2016/8/10.
  */
object LogsMain {

  def main(args: Array[String]) {

    LogManager.getRootLogger.setLevel(Level.ERROR)

    if (args.length < 2) {
      System.err.println("Usage: Spark-submit --configs <topic> <esindex>") //hadoop02_hadoop-cmf-yarn-RESOURCEMANAGER-hadoop02encrypted001logencrypted001out,/yarn1/hadoop02_yarn_resource_manager1
      sys.exit(1)
    }
    val Array(topic, esindex,_*) = args

//    val topic = "hadoop02_hadoop-cmf-yarn-RESOURCEMANAGER-hadoop02encrypted001logencrypted001out"
//    val indexname = "/yarn1/hadoop02_yarn_resource_manager1"
    val ssc = StreamingContext.getOrCreate("/data/mllib/logs", ()=>createStreamingContext(topic, esindex))

    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext(topic:String, indexname:String): StreamingContext = {
    val conf = new SparkConf().setAppName("Logs2Streaming2esWithCoreMemoryInfo")
    val ssc = new StreamingContext(conf, Seconds(60))
    ssc.checkpoint("/data/mllib/logs")

    //direct Approach(No Receivers)
    val kafkaConfig = Map("metadata.broker.list" -> "10.1.80.60:9092,10.1.80.68:9092")
    val topics = Set(topic)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConfig, topics).map(_._2)

    val flag = "ApplicationSummary"
    val eshost = "10.1.80.75"
//    JobInfo.yarnJobInfo2ES(stream, flag, indexname, eshost)
//    val jobStream = JobInfo.yarnJobInfo2PairStream(stream, flag)

    val containerStream = ContainerInfo.generatePairDStream(stream)
    val updateContainerStream = ContainerInfo.myUpdateStateByKey(containerStream)

    updateContainerStream.print()

//    val testContainer = updateContainerStream.map{i=> i._1+i._2}

//    val schema = StructType(Array(StructField("id", StringType, true), StructField("content", StringType, true)))

//    JobInfo.stringRDD2ES(updateContainerStream, schema, indexname, eshost)

//    JobInfo.jsonRDD2ES(testContainer, indexname, eshost)

//    JobContainerInfo.update2ES(jobStream, updateContainerStream, indexname, eshost)

    ssc
  }

}
