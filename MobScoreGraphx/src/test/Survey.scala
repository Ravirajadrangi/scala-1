package test

import scala.collection.mutable.Set
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD



/**
  * Created by dyh on 2016/8/23.
  */
object Survey {

  val conf = new SparkConf().setAppName("Logs2Streaming2esWithCoreMemoryInfo")
  val sc = new SparkContext(conf)

  val users: RDD[(VertexId, (Boolean, Boolean, String))] = {    //(isRegistered, isBlack, info)
    sc.parallelize(Array((5L, (false, false, "")),
      (4L, (false, false, "")), (3L, (false, true, "")),
      (6L, (false, false, "")), (1L, (true, false, "")),
      (2L, (true, false, ""))))}

  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(5L, 1L, ""),Edge(1L, 3L, ""), Edge(1L, 4L, ""),
      Edge(3L, 2L, ""), Edge(4L, 2L, ""), Edge(2L, 6L, "")))

  val defaultUser = (false, false, "Missing")

  val graph = Graph(users, relationships, defaultUser)

  val facts:RDD[String] = graph.triplets.map{triplet =>
    triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
  }

  val regEdge = graph.triplets.filter{triplet =>
    triplet.srcAttr._1 || triplet.dstAttr._1
  }

  val firstRelation = graph.triplets.filter{triplet =>
    triplet.srcAttr._1 || triplet.dstAttr._1    //筛选注册号的边
  }.map{triplet =>                           //将边转换成(注册号, 对应号)的元组
    if(triplet.srcAttr._1&&triplet.dstAttr._1){
      (triplet.srcId, triplet.dstId, 1)
    }else if(triplet.srcAttr._1){
      (triplet.srcId, triplet.dstId, 0)
    }else if(triplet.dstAttr._1){
      (triplet.dstId, triplet.srcId, 0)
    }else{
      (0L, 0L, 0)
    }
  }.flatMap{x => x._3 match {
    case 1 => Array(x, (x._2, x._1, x._3))
    case 0 => Array(x)
  }}.map{x=>(x._1, x._2.toString)}.reduceByKey{
    (a, b) => a +"|" + b
  }


  val neighborIds = graph.collectNeighborIds(org.apache.spark.graphx.EdgeDirection.Either)


  //计算二级关系
  val nbrSets: VertexRDD[Set[VertexId]] = graph.collectNeighborIds(org.apache.spark.graphx.EdgeDirection.Either).mapValues{
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

  val sg = GraphImpl(nbrSets, graph.edges)


  def edgeFunc(ctx: EdgeContext[Set[VertexId], String, Set[VertexId]]): Unit = {
    var msg = ctx.srcAttr
    msg = msg.union(ctx.dstAttr)
    ctx.sendToSrc(msg)
    ctx.sendToDst(msg)
  }


  val n2Neigh = sg.aggregateMessages(edgeFunc, (a:Set[VertexId], b:Set[VertexId]) => a.union(b))

}
