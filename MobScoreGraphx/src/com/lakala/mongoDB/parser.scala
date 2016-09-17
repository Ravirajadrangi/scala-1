package com.lakala.mongoDB



import scala.util.matching._
import com.google.gson.JsonParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source



/**
  * Created by dyh on 2016/9/8.
  */
class Parser {

  def parseFirstLevel(jsonLine:String,firstKeys:Array[String]):Array[String] = {

    val jsonParser = new JsonParser()
    val LogJson = jsonParser.parse(jsonLine).getAsJsonObject()

    var firstFields:Array[String] = Array()
    for(key <- firstKeys){
      firstFields = firstFields :+ LogJson.get(key).toString
    }

    firstFields
  }

  def parseSecondLevel(jsonLine:String, secondKeys:Array[String]):Array[Array[String]] = {

    val jsonParser = new JsonParser()
    val LogJson = jsonParser.parse(jsonLine).getAsJsonObject()
    val contacts:String = LogJson.get("mode").getAsString

    var con:Array[Array[String]] = Array()
    try {
      val contactsJson = jsonParser.parse(contacts).getAsJsonArray
      for (i <- 0 until (contactsJson.size())) {
        val elJson = contactsJson.get(i).getAsJsonObject
        var secondFields: Array[String] = Array(LogJson.get("loginName").getAsString) //加上通讯录主人的手机号
        for (key <- secondKeys) secondFields = secondFields :+ elJson.get(key).toString
        con = con :+ secondFields
      }
    }catch{
      case np: NullPointerException => //println(contacts+"NullPointerException")
      case ig: IllegalStateException => println(LogJson.get("loginName")+ "IllegalStateException")

    }

    con
  }

  def parseAndroid(contacts:String, loginName:String):Array[Array[String]] = {

    val jsonParser = new JsonParser()
    var con:Array[Array[String]] = Array()
    try {
      val contactsJson = jsonParser.parse(contacts).getAsJsonArray
      for (i <- 0 until (contactsJson.size())) {
        val elJson = contactsJson.get(i).getAsJsonObject
        var secondFields: Array[String] = Array(loginName) //加上通讯录主人的手机号
        secondFields = secondFields :+ elJson.get("Phone").toString
        con = con :+ secondFields
      }
    }catch{
      case np: NullPointerException => //println(contacts+"NullPointerException")
    }
    con
  }

  def parseIOS(contacts:String, loginName:String):Array[Array[String]] ={

    val jsonParser = new JsonParser()
    var con:Array[Array[String]] = Array()

    try {
      val contactsJson = jsonParser.parse(contacts).getAsJsonArray
      for (i <- 0 until (contactsJson.size())) {
        val elJson = contactsJson.get(i).getAsJsonObject.get("Phone").toString
        println(elJson)
        var secondFields: Array[String] = Array(loginName) //加上通讯录主人的手机号
        secondFields = secondFields :+ elJson
        con = con :+ secondFields
      }
    }catch {
      case np: NullPointerException => //println(contacts+"NullPointerException")
      case ig: IllegalStateException => //println(loginName+ "IllegalStateException")   //尚未解析完全
    }
    con
  }

  def parseSecondLevel(jsonLine:String):Array[Array[String]] ={     //不详细解析

    val jsonParser = new JsonParser()

    val LogJson = jsonParser.parse(jsonLine).getAsJsonObject()
    val contacts:String = LogJson.get("mode").getAsString
    val loginName:String = LogJson.get("loginName").getAsString

    var con:Array[Array[String]] = Array()


    LogJson.get("platform").getAsString match {
      case "Android" => con = parseAndroid(contacts, loginName)
      case "iOS" => con = parseIOS(contacts, loginName)
      case _ =>
    }
    con
  }

  def parseThirdLevel(phoneStr:String, key:String):Array[Array[String]]={

    val regex1 = new Regex("\"([\\d].*?)\"")
    val regex2 = new Regex("[\u4e00-\u9fa5]")       //匹配中文
    val regex3 = new Regex(" |-")    //匹配空格和-
    val result1 = regex1.findAllMatchIn(phoneStr)

    var con:Array[Array[String]] = Array()
    for(i <- result1){
      val regex1(phone) = i
      val phoneWithoutC = regex2.replaceAllIn(phone, "")     //去除中文
      if(phone.length==phoneWithoutC.length){
        val phone1 = regex3.replaceAllIn(phone, "")     //181-0328-3333 和181 0328 3333
        try{
          val t = phone1.toLong               //去除不是手机号的部分
          if(phone1.length<=10&&phone1.length>=6) con = con :+ Array(key, phone1)
        }catch {
          case e:NumberFormatException => phone1 + " is't mobile number"
        }
      }
    }
    con
  }





  /**
    * 将RDD转换成带schema的DataFrame
    *
    * @param rdd
    * @param keys: schema 中的字段名
    * @return df: 返回DataFrame
    */
  def getDataFrame(rdd:RDD[Array[String]], keys:Array[String]):DataFrame = {

    var schemaArr:Array[StructField] = Array()
    for(key <- keys) schemaArr = schemaArr :+ StructField(key, StringType, true)
    val schema = StructType(schemaArr)

    val rddRow = rdd.map{arr =>
      Row.fromSeq(arr.toSeq)
    }

//    val sqlContext = new SQLContext(rdd.sparkContext)
//    val df = sqlContext.createDataFrame(rddRow, schema)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(rdd.sparkContext)
    val df = hiveContext.createDataFrame(rddRow, schema)
    df
  }


}


object Parser{

  val parser = new Parser()

  def parseFirstLevel(rdd:RDD[String], keys:Array[String]):DataFrame = {

    val parsedRDD = rdd.map(line => parser.parseFirstLevel(line, keys))
    parser.getDataFrame(parsedRDD, keys)
  }


  def parseSecondLevel(rdd:RDD[String], keys:Array[String]):DataFrame = {

//    val parsedRDD = rdd.map(parser.parseSecondLevel(_, keys))
    val parsedRDD = rdd.map(parser.parseSecondLevel(_))
    val flattedRDD = parsedRDD.flatMap{x => x}
    val keysWithMaster:Array[String] = "master" +: keys       //加通讯录主人的手机号
    parser.getDataFrame(flattedRDD, keysWithMaster)
  }

  def parseThirdLevel(df:DataFrame, keys:Array[String]):DataFrame = {

    val parsedRDD = df.rdd.map(row => parser.parseThirdLevel(row(1).toString, row(0).toString))
    val flattedRDD = parsedRDD.flatMap{x => x}
    parser.getDataFrame(flattedRDD, keys)
  }

  def main(args: Array[String]) {

    val jsonParser = new JsonParser()

    val path = "C:\\Users\\yn\\Desktop\\dyh_test.txt"
    Source.fromFile(path).getLines.foreach{line=>
      val LogJson = jsonParser.parse(line).getAsJsonObject()
      val contacts:String = LogJson.get("mode").getAsString
      val contactsJson = jsonParser.parse(contacts).getAsJsonArray

      var con:Array[Array[String]] = Array()
      for (i <- 0 until (contactsJson.size())) {
        val elJson = contactsJson.get(i).getAsJsonObject.get("Phone").toString
        println(elJson)
        var secondFields: Array[String] = Array(LogJson.get("loginName").getAsString) //加上通讯录主人的手机号
        secondFields = secondFields :+ elJson
        con = con :+ secondFields
      }
      println(con)
    }

  }

}