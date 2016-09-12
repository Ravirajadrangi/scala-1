package lakala.spark.logs.yarn.resourceManager



import scala.util.matching._
import org.apache.spark.streaming.dstream.DStream
/**
  * Created by dyh on 2016/8/15.
  */
class ContainerInfo {

  /**
  提取带有flag字符串的行，在提取container申请的内存和核数时flag='SchedulerNode: Assigned'
  dstream
  flag:所提取的行中包含的字段
  * */
  def extractContainer(dstream: DStream[String], flag: String): DStream[String] = {

    val jobInfo = new JobInfo()
    val purifiedStream = jobInfo.extractSummary(dstream, flag) //'SchedulerNode: Assigned'
    purifiedStream
  }

  /**
  * 提取container所属任务的id号，以及内存数、核数、主机号
  * dstream
  * */
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

  /**
    * 对一个任务的所有container信息进行聚合，mem、core相加, host叠加
    */
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

object ContainerInfo{

  val containerInfo = new ContainerInfo()

  def generatePairDStream(dstream:DStream[String]):DStream[(String, String)]={

    val container = containerInfo.extractContainer(dstream, "SchedulerNode: Assigned")
    val containerDetail = containerInfo.extractDetailInfo(container)
    containerDetail
  }

  def myUpdateStateByKey(dstream: DStream[(String, String)]):DStream[(String, String)]={
    val updateStream = containerInfo.myUpdateStateByKey(dstream)
    updateStream
  }

}

