package com.lakala.contacts

import com.google.gson.{Gson}
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.sql._

import scala.collection.mutable.Set


case class Contacts1(vid:String, neighbors:Array[String])

/**
  * Created by dyh on 2016/9/14.
  */
class Relationship{

  def createGraph(df:DataFrame):Graph[String, String] = {

    val vertex: RDD[(VertexId, String)] = df.rdd.map(row=>row.toSeq).flatMap(x=>x).map{x=>
      (x.toString.toLong, "")
    }

    val Edges: RDD[Edge[String]] = df.rdd.map{ row =>
          Edge(row(0).toString.toLong, row(1).toString.toLong, "")
    }

    val defaultUser = ("Missing")

    val graph = Graph(vertex, Edges, defaultUser)

    graph
  }

  def firstDegreeGraph(graph:Graph[String, String]):Graph[Set[VertexId], String] = {

    val nbrSets1: VertexRDD[Set[VertexId]] = graph.collectNeighborIds(org.apache.spark.graphx.EdgeDirection.Either).mapValues{
      (vid, nbrs) =>
        val set = Set[VertexId]()
        var i = 0
        while(i<nbrs.size){
          if(nbrs(i) != vid){
            set.add(nbrs(i))
          }
          i += 1
        }
        set
    }

    val sg1 = GraphImpl(nbrSets1, graph.edges)
    sg1
  }

  def secondDegreeNeighbors(graph:Graph[String, String]):VertexRDD[Set[VertexId]] = {

    val sg1:Graph[Set[VertexId], String] = firstDegreeGraph(graph)

    val edgeFunc1 = (ctx: EdgeContext[Set[VertexId], String, Set[VertexId]]) => {
            var msg2dst = ctx.srcAttr
            msg2dst = msg2dst - ctx.dstId
            ctx.sendToDst(msg2dst)

            var msg2src = ctx.dstAttr
            msg2src = msg2src - ctx.srcId
            ctx.sendToSrc(msg2src)
          }


//    def edgeFunc1(ctx: EdgeContext[Set[VertexId], String, Set[VertexId]]): Unit = {      //问题所在
//      var msg2dst = ctx.srcAttr
//      msg2dst = msg2dst - ctx.dstId
//      ctx.sendToDst(msg2dst)
//
//      var msg2src = ctx.dstAttr
//      msg2src = msg2src - ctx.srcId
//      ctx.sendToSrc(msg2src)
//    }
//
    val n2Neigh1 = sg1.aggregateMessages(edgeFunc1, (a:Set[VertexId], b:Set[VertexId]) => a.union(b))

    n2Neigh1
  }

  def getJsonRDD(vertex:(Long, Set[VertexId])):String={

    val contacts1 = Contacts1(vertex._1.toString, vertex._2.map(i=>i.toString).toArray)
    val gson = new Gson()
    val jsonStr = gson.toJson(contacts1)
    jsonStr
  }

}



object Relationship extends Serializable{

  val relationship = new Relationship()

  def getFirstDegreeNeighbors(df:DataFrame):Graph[Set[VertexId], String]={

    val graph: Graph[String, String] = relationship.createGraph(df)
    val fs = relationship.firstDegreeGraph(graph)
    fs
  }

  def getSecondDegreeNeighbors(df:DataFrame):RDD[String]= {

    val graph: Graph[String, String] = relationship.createGraph(df)
    val vertex = relationship.secondDegreeNeighbors(graph)
    val rdd = vertex.map(relationship.getJsonRDD(_))
    rdd
  }

  def jsonRDD2ES(rdd:RDD[String], indexname:String, eshost:String):Unit={

    val sqlContext = new SQLContext(rdd.sparkContext)
    val jsonRDD = sqlContext.read.json(rdd)
    jsonRDD.saveToEs(Map(ES_RESOURCE_WRITE->indexname, ES_NODES->eshost,ES_MAPPING_ID->"vid"))
  }
}



