package realTimeSimulation.joinDFStreaming

import realTimeSimulation.filterStreaming.filterStreaming1
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.Row
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.sql._

/**
  * Streaming中的rdd与配置表关联(卡号cardno),并获得六个字段，如下
  * "user_id": "1000067240",
  * "mobile_num": "18637173180",
  * "cardno": "6236682430000215069",
  * "trans_date": "2015-09-26T06:15:10Z",
  * "trans_name": null,
  * "total_am": "3200000"
  */

object joinDFStreaming1 {
  def getFullFields(stream: DStream[Row],configDF:DataFrame,schema: StructType): Unit = {
    stream.foreachRDD{rdd=>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      val toDF = sqlContext.createDataFrame(rdd, schema)
      if(toDF.count!=0) {
        println(toDF.first + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      }

//      val name:String = "graphx.user_id_mobile_num_4streaming"
//      val Hivetable:String = s"SELECT * FROM $name"
//      //    val userMob = getDataFrame.getDF(ssc, s"select * from $table") //配置表
//      val userMob = sqlContext.sql(Hivetable)

      val finalDF = toDF.join(configDF, configDF("cardno")===toDF("cardno1"),"inner").drop("cardno1")


      if(finalDF.count!=0) {
        println(finalDF.first + "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
      }
      finalDF.saveToEs(Map(ES_RESOURCE_WRITE -> "test/pos", ES_NODES -> "10.1.80.75"))
    }
  }
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