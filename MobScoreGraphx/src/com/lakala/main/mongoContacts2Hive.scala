package com.lakala.main

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}


import com.lakala.mongoDB.Parser
/**
  * Created by yn on 2016/9/8.
  */
object mongoContacts2Hive {

  def main(args: Array[String]) {

    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("mongoContacts2Hive")
    val sc = new SparkContext(conf)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)


//    val rawContacts = sc.textFile("/data/auto/contactlist.txt")
    val rawContacts = sc.textFile("/data/auto/contactlist1")

    val firstKeys:Array[String] = Array("collectTime", "loginName", "deviceId",
      "osVersion", "subChannelID", "_id", "deviceModel",
      "friends", "appVersion", "platform", "mode", "_class","type", "readTime")

    val firstLevelDF = Parser.parseFirstLevel(rawContacts, firstKeys)

    val secondKeys:Array[String] = Array("DISPLAY_NAME", "Relation", "Im", "Phone", "Email",
      "Nickname", "StructuredPostal", "ID", "Organization", "Note")

    val secondLevelDF = Parser.parseSecondLevel(rawContacts, secondKeys)


    val thirdKeys:Array[String] = Array("master", "contact")
    val thirdLevelDF = Parser.parseThirdLevel(secondLevelDF, thirdKeys)

    hiveContext.sql("use graphx")

    thirdLevelDF.write.mode(org.apache.spark.sql.SaveMode.Append).saveAsTable("contacts_edge")
  }
}
