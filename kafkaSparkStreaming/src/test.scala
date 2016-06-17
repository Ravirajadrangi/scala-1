
import java.net.URLDecoder
import com.google.gson._
import java.math.{BigDecimal=>myBD}

import tools.testAccurate
import timeTransform.timeTransform

import scala.io.Source

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

object test {
  def main(args: Array[String]) {
    val ac = testAccurate
    val fileIter = Source.fromFile("E:\\deny\\ML\\python\\xiaodai\\20160513_mobile-access-log_.1463137199455.tmp").getLines()


    while(fileIter.hasNext) {
      val teline = fileIter.next()
//      try {
        getWantedPage(teline)
//      }catch{
//        case ex:Exception => println(ex)
//      }
    }
    }

  def getWantedPage(line:String):Option[Row]={
    var row = Row("")
    val ac = testAccurate
    val tedecoded = URLDecoder.decode(line, "utf-8").mkString
    val jsonStr = ac.findJson(tedecoded)
    val gson = new JsonParser
    val teJson: JsonObject = gson.parse(jsonStr).getAsJsonObject

    val isMobil = ac.isMobile(teJson)
    if (isMobil) {

      try {
        val events = teJson.getAsJsonObject("events")
        val jsonSome:Option[JsonArray] = if(events.has("event")) Some(events.getAsJsonArray("event")) else None  //Json中是否有event

        val jsonArr:JsonArray = jsonSome match{
          case Some(a) => a
          case None => new JsonArray()
        }

        if(jsonArr.size()!=0) {
          for (i <- 0 until jsonArr.size()) {
            //遍历event，挑出符合要求的页面
            val teMap = jsonArr.get(i)
            val isPage: Boolean = ac.isChosenPage("Credit-1", teMap)
            if (isPage) {
              val tmp: String = teMap.getAsJsonObject.get("ts").toString
              val ts = new myBD(tmp)
              val date = timeTransform.DateFormat(ts.toString)
              val mobile = teMap.getAsJsonObject.get("attributes").getAsJsonObject.get("mobile").toString.replace("\"", "")
              row = Row(mobile, date)
            }
          }
        }
      }catch{
        case ex:NullPointerException =>
      }

    }
    println(row)
    if(row.length!=0) Some(row) else None
  }

  def getMinNull(input:DataFrame)={
    val output = input.map{row=>
      Row(row(0), row.toSeq.filter(_==null).length)
    }
    output
  }
}

