
import java.math.{BigDecimal=>myBD}
import java.net.URLDecoder
import com.google.gson.{JsonArray, JsonObject, JsonParser}



import timeTransform.timeTransform
import tools.testAccurate

import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{StructType,StringType,StructField}
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.apache.log4j.{Level, LogManager}



//setMaster("local[2]").
object kafkaStreaming {
  def main(args: Array[String]) {

    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("kafka2Streaming2es")
    val ssc = new StreamingContext(conf, Seconds(60))

    //receiver-based Approach
    val zkQuorum = "10.1.80.60:2181,10.1.80.61:2181,10.1.80.65:2181,10.1.80.171:2181,10.1.80.177:2181"
    val groupId = "1"
    val topics = Map("mobile1" -> 3)
    val lines = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics).map(_._2)

    val decoded = lines.map(line => processStreaming.getWantedPage(line)).filter(_ != None).map(_.get)


    val myschema = StructType(Array(StructField("pk_mobile", StringType, true), StructField("date", StringType, true)))

    processStreaming.toES(decoded, myschema, "streaming/test", "10.1.80.75")

    ssc.start()
    ssc.awaitTermination()
  }


  object processStreaming {
    /*解析spark streaming的数据，然后写入ES中*/
    def getWantedPage(line: String) = {
      var row = Row("")
      val ac = testAccurate
      val tedecoded = URLDecoder.decode(line, "utf-8").mkString
      val jsonStr = ac.findJson(tedecoded)
      val gson = new JsonParser
      try {
        val teJson: JsonObject = gson.parse(jsonStr).getAsJsonObject


        val isMobil = ac.isMobile(teJson)
        if (isMobil) {
          val events = teJson.getAsJsonObject("events")
          val jsonSome: Option[JsonArray] = if (events.has("event")) Some(events.getAsJsonArray("event")) else None //Json中是否有event
          val jsonArr: JsonArray = jsonSome match {             //完全没有必要啊
              case Some(a) => a
              case None => new JsonArray()
            }

          val jsonArrLen = jsonArr.size()
          if (jsonArrLen > 0) {
            try {
              var teMap = jsonArr.get(0)
              for (i <- 0 until jsonArr.size()) {
                //遍历event，挑出符合要求的页面
                teMap = jsonArr.get(i)
                val isPage: Boolean = ac.isChosenPage("Credit-1", teMap)
                if (isPage) {
                  val tmp: String = teMap.getAsJsonObject.get("ts").toString
                  val ts = new myBD(tmp)
                  var date = timeTransform.DateFormat(ts.toString)
                  date = timeTransform.toESDate(date)
                  val mobile = teMap.getAsJsonObject.get("attributes").getAsJsonObject.get("mobile").toString.replace("\"", "")
                  row = Row(mobile, date)
                }
              }
            } catch {
              case ex: IndexOutOfBoundsException =>

            }
          }
        }
      } catch {
        case ex: NullPointerException => println(jsonStr)
      }
      if (row.length != 1) Some(row) else None
    }

    /** Lazily instantiated singletion instance of SQLContext **/
    object SQLContextSingleton {
      @transient private var instance: SQLContext = _

      def getInstance(sparkContext: SparkContext): SQLContext = {
        if (instance == null) {
          instance = new SQLContext(sparkContext)
        }
        instance
      }
    }

    def toES(data: DStream[Row], schema: StructType, indexname: String, eshost: String): Unit = {
      data.foreachRDD { rdd =>
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

        val toDF = sqlContext.createDataFrame(rdd, schema)

        val num: Long = toDF.count()
        if (num != 0) {
          toDF.saveToEs(Map(ES_RESOURCE_WRITE -> indexname, ES_NODES -> eshost))
        }
      }
    }
  }
}