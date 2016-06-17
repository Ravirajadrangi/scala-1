
import hive2ES.SQLContextSingleton

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, DataFrame}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.sql._

/**
  * 将hive中的数据不经过spark的处理直接写放到ES中
  */
object hive2ESWithoutProcess {
  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("flume_streaming1")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val DataFrame1 = sqlContext.sql("select * from yxt.pos_atmtxnjnl_success_1y")    //150万的数据量
    val DataFrame2 = sqlContext.sql("select * from yxt.cp_trans_success_1y")      //7500万的数据量

    val dataFrame1Rename = DataFrame1.withColumnRenamed("txn_data", "data_date")

    val FirstDF = processed.toESDate(dataFrame1Rename, 2)
    val SecondDF = processed.toESDate(DataFrame2, 2)

    FirstDF.saveToEs(Map(ES_RESOURCE_WRITE->"yxt/pos_atmtxnjnl_success_1y",ES_NODES->"10.1.80.75"))
    SecondDF.saveToEs(Map(ES_RESOURCE_WRITE->"yxt/cp_trans_success_1y",ES_NODES->"10.1.80.75"))
  }
}

object processed {
  def toESDate(d: DataFrame, site: Int): DataFrame = {
    val data = d.map { row =>
      val newRow = row.toSeq.toBuffer
      val te = toESDate(newRow(site).toString)
      newRow(site) = te
      Row.fromSeq(newRow.toSeq)
    }
    val sqlContext = SQLContextSingleton.getInstance(d.rdd.sparkContext)   //data.sqlContext
    val dateDF = sqlContext.createDataFrame(data, d.schema)
    dateDF
  }

  def toESDate(date:String):String={
    /*ES会将yyyy-MM-ddTHH:mm:ssZ格式的数据视作日期，如2014-08-01T03：27：33Z被识别成字符串， 而2014-08-01 12：00：00
    * 这样的字符串却不会被识别成日期*/
    val tmp:String = date.split(" ").mkString("T") + "Z"
    tmp
  }
}