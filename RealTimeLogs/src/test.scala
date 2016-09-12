

import kafka.serializer.StringDecoder


import scala.util.matching._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dyh on 2016/8/10.
  */
object test {

  def main(args: Array[String]) {

    val a = """2016-08-16 02:41:25,304 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode: Assigned container container_e127_1470819460823_57029_01_000002 of capacity <memory:1024, vCores:1> on host hadoop01:8041, which has 3 containers, <memory:3072, vCores:3> used and <memory:48128, vCores:15> available after allocation"""
    val b = StringEscapeUtils.unescapeJava(a)
    val c:String = "<memory:1024, vCores:1>"

    val regex1 = new Regex("<memory:([\\d]+), vCores:([\\d]+)>")
    val regex2 = new Regex("host (hadoop[\\d]+):")
    val regex3 = new Regex("container (container_[\\S]+)")

    val (me, co) = regex1.findFirstIn(a) match{
      case Some(s) => val regex1(mem, core) = s; (mem, core)
      case None => ("", "")
    }
    println(me + "  " + co)

    val host = regex2.findFirstIn(a) match{
      case Some(s) => val regex2(host) = s; host
      case None => ""
    }

    println(host)

//    println(regex3.findFirstIn(a))
    val id = regex3.findFirstIn(a) match{
      case Some(s) => val regex3(container) = s; val id = container.split("_").slice(2, 4).mkString("_");id
      case None => ""
    }

    println(id)

    val B = (s:String) => s match {
      case "" => false
      case _ => true
    }

    if(B(co) && B(me) && B(host) && B(id)){
      println("aaaaa")
    }else{
      println("bbbbb")
    }


    val tSeq = Seq(Array("1", "2", "hadoop02"), Array("5", "8", "hadoop08"))

    val tResult:Array[String] = tSeq.reduce{
      (a, b) => {
        val first = a(0).toInt + b(0).toInt
        val second = a(1).toInt + b(1).toInt
        val third = a(2) + "\001" + b(2)
        Array(first.toString, second.toString, third)
      }

    }

    println(tResult)
    tResult.foreach(println(_))

    val sApp = "abc"
//    val sApp = """"{"appId":"application_1470819460823_10936","name":"create table wm.ydjr_zx_repor...cert_no","name(Stage-1)","user":"hdfs","queue":"default","state":"FINISHED","trackingUrl":"http://hadoop01:8088/proxy/application_1470819460823_10936/jobhistory/job/job_1470819460823_10936","appMasterHost":"hadoop04","startTime":"1470904747426","finishTime":"1470904791066","finalStatus":"SUCCEEDED"}""""
    val regexSapp = new Regex("application_([\\d]+_[\\d]+)")
    val regexSapp(appId) = regexSapp.findFirstIn(sApp).getOrElse("application_0_0")
    println(appId)




    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint("/data/mllib/logs")

    val topic = "hadoop02_hadoop-cmf-yarn-RESOURCEMANAGER-hadoop02encrypted001logencrypted001out"
    val indexname = "/yarn1/hadoop02_yarn_resource_manager1"

    val kafkaConfig = Map("metadata.broker.list" -> "10.1.80.60:9092,10.1.80.68:9092")
    val topics = Set(topic)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConfig, topics).map(_._2)

    val containerStream = test1.generatePairDStream(stream)

    val updateContainerStream = UpdateByKey.myUpdateStateByKey(containerStream)

    updateContainerStream.print()


    ssc.start()
    ssc.awaitTermination()
  }
}




object UpdateByKey{

  def myUpdateStateByKey(dstream: DStream[(String, String)]) = {

    //定义状态更新函数（聚合函数）
    val updateFunc = (values: Seq[String], stat: Option[String]) => {
      val preState:String = stat.getOrElse[String]("0\0010\0010")
      val tmp:Seq[String] = values :+ preState

      val result = tmp.reduce {
        (a: String, b: String) => {
          val mySplit = (s:String) => s.split("\001")
          val (a1, b1) = (mySplit(a), mySplit(b))

          val first = a1(0).toInt + b1(0).toInt
          val second = a1(1).toInt + b1(1).toInt
          val third = a1(2) + "|" + b1(2)

          val re = first.toString+ "\001" +second.toString+ "\001" +third
          re
        }
      }
      Some(result)
    }

    val stateDstream = dstream.updateStateByKey(updateFunc)

    stateDstream
  }
}


object test1{

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

  def extractContainer(dstream: DStream[String], flag: String): DStream[String] = {

    val purifiedStream = extractSummary(dstream, flag) //'SchedulerNode: Assigned'
    purifiedStream
  }

  def extractDetailInfo(dstream: DStream[String]): DStream[(String, String)] = {

    val regexCon = new Regex("container_[\\S]*")
    val regex1 = new Regex("<memory:([\\d]+), vCores:([\\d]+)>")
    val regex2 = new Regex("host (hadoop[\\d]+):")
    val regex3 = new Regex("container (container_[\\S]+)")

    val memCoreDstream = dstream.map { line =>
      val (mem, core) = regex1.findFirstIn(line) match {
        case Some(a) => val regex1(mem, core) = a; (mem, core)
        case None => ("", "")
      }

      val host = regex2.findFirstIn(line) match {
        case Some(s) => val regex2(host) = s; host
        case None => ""
      }

      val id = regex3.findFirstIn(line) match {
        case Some(s) => val regex3(container) = s; val id = container.split("_").slice(2, 4).mkString("_"); id
        case None => ""
      }

      //过滤掉id,mem,core, host任一项为空的项
      val B = (s: String) => s match {
        case "" => false
        case _ => true
      }

      val fields = Array(mem, core, host)
      val tmpS = fields.mkString("\001")

      if (B(mem) && B(core) && B(host) && B(id)) {
        (id, tmpS)
      } else {
        ("0", "0")
      }
    }.filter { tup =>
      tup._1 match {
        case "0" => false
        case _ => true
      }
    }

    memCoreDstream
  }

  def generatePairDStream(dstream:DStream[String]):DStream[(String, String)]={

    val container = extractContainer(dstream, "SchedulerNode: Assigned")
    val containerDetail = extractDetailInfo(container)
    containerDetail
  }
}