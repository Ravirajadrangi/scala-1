package com.lakala.mongoDB


import com.google.gson.JsonParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}



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
    val contactsJson = jsonParser.parse(contacts).getAsJsonArray

    var con:Array[Array[String]] = Array()
    for(i <- 0 until(contactsJson.size())) {
      val elJson = contactsJson.get(i).getAsJsonObject
      var secondFields: Array[String] = Array()
      for (key <- secondKeys) secondFields = secondFields :+ elJson.get(key).toString
      con = con :+ secondFields
    }

    con
  }

  /**
    * 将RDD转换成带schema的DataFrame
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

    val sqlContext = new SQLContext(rdd.sparkContext)
    val df = sqlContext.createDataFrame(rddRow, schema)
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

    val parsedRDD = rdd.map(parser.parseSecondLevel(_, keys))
    val flattedRDD = parsedRDD.flatMap{x => x}
    parser.getDataFrame(flattedRDD, keys)
  }

}