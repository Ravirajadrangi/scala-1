package com.lakala.main

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}

import com.lakala.contacts.Relationship

/**
  * Created by yn on 2016/9/17.
  */
object contacts2Neighbors2ES {

  def main(args: Array[String]) {

    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("contacts2Neighbors")
    val sc = new SparkContext(conf)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val df = hiveContext.sql("select * from graphx.contacts_edge")

//    val fg = Relationship.getFirstDegreeNeighbors(df)
//
//    fg.vertices.count

    val rdd = Relationship.getSecondDegreeNeighbors(df)

    rdd.count()

//    Relationship.jsonRDD2ES(rdd, "/graphx_test/contacts2Neighbors", "10.1.80.75")
  }

}
