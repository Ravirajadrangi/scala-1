package hive2ES.onlineSearch.everyday2ES


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, Row}
import hive2ES.onlineSearch.utils.SQLContextSingleton
import org.apache.spark.sql.hive.HiveContext
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.sql._

/**
  * Created by dyh on 2016/8/26.
  */
class OnlineSearch2ES {

  def getPartitionName(df: DataFrame, partitionField: String): Array[String] = {

    //val a = sqlContext.sql("select * from yxt.cp_trans_success_tele")
    //val col = a.col("ymd").getItem(2)
    val uniqNames = df.rdd.map { row =>
      val p = row(row.length - 1)
      p.toString //事出紧急
    }.mapPartitions { iter =>

      val list = iter.toSet.toList
      list.iterator
    }.distinct

    val uniqNamesArr = uniqNames.collect

    uniqNamesArr
  }

  def getParNameFromHive(df: DataFrame, hiveTable: String): Array[String] = {

    val hiveContext = new HiveContext(df.rdd.sparkContext)
    val nameDf = hiveContext.sql(s"show partitions $hiveTable")
    val namesArr = nameDf.rdd.map { row => row(0).toString.split("=")(1) }.collect()
    namesArr
  }

  def getPartitionData(df: DataFrame, partitionName: String): DataFrame = {

    val pDf = df.where(s"ymd=$partitionName").drop("ymd")
    pDf
  }

  /**
    * 全量的向ES中灌入数据
    * df：hive表中读出的数据
    * partitionNames：分区名
    * esIndex：ES的索引名，如 yxt/ulb_collect_all
    **/
  def all2ES(df: DataFrame, partitionNames: Array[String], esIndex: String): Unit = {

    for (ymd: String <- partitionNames) {
      var data = OnlineSearch2ES.getPartitionData(df, ymd)
      data = processed1.toESDate(data, 2)
      val newIndex = esIndex + "_" + ymd.slice(0, 6) //year and month
      data.saveToEs(Map(ES_RESOURCE_WRITE -> newIndex, ES_NODES -> "10.1.80.75"))
    }
  }

  /** 向ES中灌入指定分区的数据
    * df：hive表中读出的数据
    * allPartition：hive表中所有的分区号
    * partitionNames：分区名,用","分割， 如 20160506,20160705
    * esIndex：ES的索引名，如 yxt/ulb_collect_all
    * */
  def special2ES(df: DataFrame, allPartition: Array[String], partitionNames: String,
                 esIndex: String): Unit = {

    val pNames = partitionNames.split(",");
    for (y: String <- pNames) {
      val ymd = StringUtils.strip(y)
      if (allPartition.contains(ymd)) {
        var data = OnlineSearch2ES.getPartitionData(df, ymd)
        data = processed1.toESDate(data, 2)
        val newIndex = esIndex + "_" + ymd.slice(0, 6) //year and month
        data.saveToEs(Map(ES_RESOURCE_WRITE -> newIndex, ES_NODES -> "10.1.80.75"))
      }
    }
  }

  /** 向ES中灌入指定日期分区之后的数据
    * df：hive表中读出的数据
    * allPartition：hive表中所有的分区号
    * startPartition：开始的分区名
    * esIndex：ES的索引名，如 yxt/ulb_collect_all
    */
  def increment2ES(df: DataFrame, allPartition: Array[String], startPartition: String,
                   esIndex: String): Unit = {

    val sp = StringUtils.strip(startPartition)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal: Calendar = Calendar.getInstance

    val nowDate: Date = new Date
    var flag: Boolean = true

    var partitionNames: Array[String] = Array();

    try {
      //获取传入分区号起始到当前的分区号
      cal.setTime(sdf.parse(sp))
      while (flag) {
        if (cal.getTime.getTime < nowDate.getTime) {
          val t1: String = sdf.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, 1)
          partitionNames = partitionNames :+ t1
        }
        else {
          flag = false
        }
      }
    }
    catch {
      case e: Exception => e.fillInStackTrace()
    }

    for (ymd: String <- partitionNames) {
      if (allPartition.contains(ymd)) {
        var data = OnlineSearch2ES.getPartitionData(df, ymd)
        data = processed1.toESDate(data, 2)
        val newIndex = esIndex + "_" + ymd.slice(0, 6) //year and month
        data.saveToEs(Map(ES_RESOURCE_WRITE -> newIndex, ES_NODES -> "10.1.80.75"))
      }
    }
  }
}

object OnlineSearch2ES{

  val onlineSearch2ES = new OnlineSearch2ES()

  def getPartitionName(df:DataFrame, hiveTable:String):Array[String]={

//    onlineSearch2ES.getPartitionName(df, "ymd")
    onlineSearch2ES.getParNameFromHive(df, hiveTable)
  }

  def getPartitionData(df:DataFrame, partitionName:String):DataFrame={

    onlineSearch2ES.getPartitionData(df, partitionName)
  }

  def yxt2ES(df:DataFrame, indexname:String, eshost:String):Unit={

    val inputData = processed1.toESDate(df, 2)
    inputData.saveToEs(Map(ES_RESOURCE_WRITE -> indexname, ES_NODES -> eshost))
  }

  def all2ES(df: DataFrame, partitionNames: Array[String], esIndex: String):Unit={

    onlineSearch2ES.all2ES(df, partitionNames, esIndex)
  }

  def special2ES(df: DataFrame, allPartition: Array[String], partitionNames: String,
                 esIndex: String): Unit ={

    onlineSearch2ES.special2ES(df, allPartition, partitionNames, esIndex)
  }

  def increment2ES(df: DataFrame, allPartition: Array[String], startPartition: String,
                   esIndex: String): Unit ={

    onlineSearch2ES.increment2ES(df, allPartition, startPartition, esIndex)
  }
}



object processed1 {
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


