package lakala.spark.logs.yarn.resourceManager

import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

/**
  * Created by yn on 2016/8/18.
  */
class JobContainerInfo {

  private def myUpdateStateByKey(dstream1:DStream[(String, String)],
                         dstream2:DStream[(String, String)]):DStream[(String,String)]={

    val updateFunc = (values:Seq[String], stat:Option[String]) => {
      val tmpValues = stat match{
        case Some(s) => values :+ s
        case None => values
      }

      var re = ""
      if(tmpValues.length==2){
        re = tmpValues.mkString("|") + "true"   //测试
      }else{
        re = tmpValues.mkString("|")
      }

      Some(re)
    }

    val joinedStream = dstream1.union(dstream2)
    val updatedStream = joinedStream.updateStateByKey(updateFunc)

    updatedStream
  }

  private def filter2ES(dstream:DStream[(String, String)], indexname:String, host:String):Unit={

    val jsonDstream = dstream.filter{case (id, line) =>
      val isend = line.endsWith("true")
      isend
    }.map{i=>i._2}

    jsonDstream.foreachRDD{rdd=>
      val sqlContext = new org.apache.spark.sql.SQLContext(rdd.sparkContext)
      val jsonRDD = sqlContext.read.json(rdd)
      jsonRDD.saveToEs(Map(ES_RESOURCE_WRITE -> indexname, ES_NODES -> host))   //"/yarn/hadoop02_yarn_resource_manager"
    }
  }
}

object JobContainerInfo{

  val jobContainerInfo = new JobContainerInfo()

  def update2ES(dstream1: DStream[(String,String)], dstream2:DStream[(String, String)],
                indexname:String, host:String):Unit={

    val hybridStream = jobContainerInfo.myUpdateStateByKey(dstream1, dstream2)
    jobContainerInfo.filter2ES(hybridStream, indexname, host)
  }
}
