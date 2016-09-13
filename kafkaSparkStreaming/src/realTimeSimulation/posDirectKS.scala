package realTimeSimulation

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import filterStreaming.filterStreaming1
import joinDFStreaming.joinDFStreaming1
import configFromHDFS.configFromHDFS1
import checkPointSSC.loadOrCreateSSC

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext}
import org.apache.spark.sql.{DataFrame, Row}


/*
* 用Spark streaming模拟生成yxt.pos_atmtxnjnl_u这张表,
* Spark streaming以direct的模式从Kafka中消费数据。
*
* */

object posDirectKS {

  def main(args: Array[String]) {

    if(args.length<1){
      System.err.println("Usage: Spark-submit --configs <topic>")   //pos1
      sys.exit(1)
    }
    val Array(topic, _*) = args

    //.setMaster("local[2]")
    LogManager.getRootLogger.setLevel(Level.ERROR)
    val ssc = StreamingContext.getOrCreate("/data/mllib/streaming", loadOrCreateSSC.createStreamingContext _)
    val sqlContext = hiveContextSingleton.getInstance(ssc.sparkContext)
    import sqlContext.implicits._


    //direct Approach(No Receivers)
    val kafkaConfig = Map("metadata.broker.list" -> "10.1.80.60:9092,10.1.80.68:9092")
    val topics = Set(topic)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConfig, topics)

    val sites = Array(6, 8, 12, 24)
    val fields: DStream[Row] = stream.map(_._2).map(line => filterStreaming1.getFields(line, sites))


    val fn = "cardno,mobile_num"
    val hdfsPath:String = s"/user/hive/warehouse/graphx.db/user_id_mobile_num_4streaming/000000_0"
    val userMob:DataFrame = configFromHDFS1.getConfigCardMob(ssc.sparkContext, hdfsPath, fn)




    val fieldNames = "trans_date,trans_name,cardno1,total_am"
    val schemaArr = fieldNames.split(",").map(field => StructField(field, StringType, true))
    val schema = StructType(schemaArr)
    joinDFStreaming1.getFullFields(fields, userMob,schema)

    fields.print()
    ssc.start()
    ssc.awaitTermination()
  }

}


/** Lazily instantiated singletion instance of hiveContext **/
object hiveContextSingleton {
  @transient private var instance: HiveContext = _

  def getInstance(sparkContext: SparkContext): HiveContext = {
    if (instance == null) {
      instance = new HiveContext(sparkContext)
    }
    instance
  }
}