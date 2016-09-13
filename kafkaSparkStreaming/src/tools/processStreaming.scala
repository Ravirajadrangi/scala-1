package tools


import java.math.{BigDecimal=>myBD}
import java.net.URLDecoder

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.streaming.dstream.DStream
import timeTransform.timeTransform
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.hive.HiveContext
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

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

  object HiveContextSingleton{
    @transient private var instance: HiveContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if(instance == null) {
        instance = new HiveContext(sparkContext)
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

  def toHive(data:DStream[Row], schema:StructType, tableName:String):Unit={
    data.foreachRDD{ rdd=>
      val hiveContext = HiveContextSingleton.getInstance(rdd.sparkContext)
      import hiveContext.implicits._
      import hiveContext.sql
      val toDF = hiveContext.createDataFrame(rdd, schema)

      val num:Long = toDF.count()
      if(num!=0){
        toDF.write.mode(Append).saveAsTable(tableName)
      }

    }
  }

  def toHdfs(data:DStream[Row], path:String):Unit={
    data.foreachRDD{rdd=>
      val num:Long = rdd.count()
      if(num!=0){
        val finalPath:String = path + "/" + timeTransform.getHour()
        val rdd1 = rdd.coalesce(1, true).map{i=>i.mkString("\001")}
        rdd1.saveAsTextFile(finalPath)
      }
    }
  }
}
