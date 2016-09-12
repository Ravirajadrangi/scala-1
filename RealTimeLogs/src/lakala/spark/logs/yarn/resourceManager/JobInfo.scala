package lakala.spark.logs.yarn.resourceManager


import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType

import scala.util.matching._
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
/**
  * Created by dyh on 2016/8/9.
  */
class JobInfo {

  def extractSummary(dstream: DStream[String], flag:String): DStream[String] = {

    val regex = new Regex(flag)     //"""ApplicationSummary"""
    val purifiedDStream = dstream.filter{line=>
      val m = regex.findFirstIn(line)
      var r = false
      if(m!=None) r = true
      r
    }
    purifiedDStream
  }

  private def extractDetailInfo(dstream:DStream[String]): DStream[String] = {

    val jsonStringDStream = dstream.map{ line=>
      val splits = line.split(": ")
      var result = splits(1).replace("\\=", "\001")   //保护\=
      result = result.replace("=", "\":\"")
      result = result.replace("\001", "\\=")    //还原\=
      result = result.replace("\\", "")
      result = result.replace(",", "\",\"")
      result = "{\"" + result +"\"}"
      result
    }
    jsonStringDStream
  }

  /**
    * 将DStream转换成PairDStream
    */
  private def pairedDstream(dstream:DStream[String]):DStream[(String, String)]={

    val regexSapp = new Regex("application_([\\d]+_[\\d]+)")
    val pDstream = dstream.map{ line =>
      val regexSapp(id) = regexSapp.findFirstIn(line).getOrElse("application_0_0")
      (id, line)
    }
    pDstream
  }


  private def jsonRDD2ES(dstream:DStream[String], indexname:String, eshost:String): Unit = {

    dstream.foreachRDD{rdd=>
      val sqlContext = new org.apache.spark.sql.SQLContext(rdd.sparkContext)
      val jsonRDD = sqlContext.read.json(rdd)
      jsonRDD.saveToEs(Map(ES_RESOURCE_WRITE -> indexname, ES_NODES -> eshost))   //"/yarn/hadoop02_yarn_resource_manager"
    }
  }

  private def stringRDD2ES(dstream:DStream[(String, String)], schema: StructType, indexname:String, eshost:String):Unit = {

    dstream.foreachRDD{rdd=>
      val sqlContext = new org.apache.spark.sql.SQLContext(rdd.sparkContext)
      val rRDD:RDD[Row] = rdd.map{t=> Row.fromSeq(Seq("working", "working"))}  //"\"\"\"" + t._2 + "\"\"\""
      val df = sqlContext.createDataFrame(rRDD, schema)
      df.saveToEs(Map(ES_RESOURCE_WRITE -> indexname, ES_NODES -> eshost))
    }
  }
}

object JobInfo{

  val jobInfo = new JobInfo()

  def yarnJobInfo2ES(dstream: DStream[String], flag:String, indexname:String, eshost:String):Unit = {

    val pDStream = jobInfo.extractSummary(dstream, flag)
    val detailDStream = jobInfo.extractDetailInfo(pDStream)
    jobInfo.jsonRDD2ES(detailDStream, indexname, eshost)
  }

  def yarnJobInfo2PairStream(dstream: DStream[String], flag:String):DStream[(String,String)] = {

    val pDStream = jobInfo.extractSummary(dstream, flag)
    val detailDStream = jobInfo.extractDetailInfo(pDStream)
    val pDstream = jobInfo.pairedDstream(detailDStream)
    pDstream
  }

  def jsonRDD2ES(dstream:DStream[String], indexname:String, eshost:String): Unit ={

    jobInfo.jsonRDD2ES(dstream, indexname, eshost)
  }

  def stringRDD2ES(dstream:DStream[(String, String)], schema: StructType, indexname:String, eshost:String):Unit ={

    jobInfo.stringRDD2ES(dstream, schema, indexname, eshost)
  }
}