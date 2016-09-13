package timeTransform

import java.text.SimpleDateFormat
import java.util.Date
object timeTransform {

  def DateFormat(ts:String):String={
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date:String = sdf.format(new Date(ts.toDouble.toLong))
    date
  }

  def toESDate(date:String):String={
    /*ES会将yyyy-MM-ddTHH:mm:ssZ格式的数据视作日期，如2014-08-01T03：27：33Z被识别成字符串， 而2014-08-01 12：00：00
    * 这样的字符串却不会被识别成日期*/
    val tmp:String = date.split(" ").mkString("T") + "Z"
    tmp
  }

  def getHour():String={
    val now: Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("HH")
    val hour = dateFormat.format(now)
    hour
  }
}
