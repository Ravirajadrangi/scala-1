package tools


import scala.collection.mutable.ArrayBuffer
import scala.util.control._
import scala.util.parsing.json.JSON
import com.google.gson._


object testAccurate {
  def findJson(line:String)={
    var stack:ArrayBuffer[Int] = new ArrayBuffer()
    var begin:Int = 0
    var end:Int = 0
    var json_str = ""
    val loop = new Breaks
    loop.breakable{
      for(i <- line.toCharArray.zipWithIndex){
        if(i._1=='{'){
          stack.append(i._2)
        }else if(i._1=='}'){
          if(stack.length==0){
            loop.break
          }else {
            try {
              begin = stack.last
              stack = stack.dropRight(1)
              end = i._2
            }catch{
              case ex: IndexOutOfBoundsException => println(stack)
            }
          }
        }
      }
    }
    if(begin!=end && end!=0){
      json_str = line.substring(begin, end+1)
    }
    json_str
//    println(json_str)
//    val myJson = JSON.parseFull(json_str)
//
//    myJson match {
//      case Some(map:Map[String, Any]) => map
//      case None => println("parse failed")
//      case other => println("unknown")
//    }
  }

  def isMobile(inJson:JsonObject) = {
    /*判断本条数据是否由手机APP产生*/
    val iosORan = Set("\"i90xl76fitu1rvvoatn02d988g8gcy850h2qzh0swuzb4o12a\"","\"i90xl76fitu1rvvoatn02d988g8gcy850h2qzh0swuzb4o12i\"")
    if(inJson.has("appid") && iosORan.exists(_==inJson.get("appid").toString)){    //如果是手机APP
      true
    }else{
      false
    }
  }

  def isChosenPage(tag:String, inJson:JsonElement)={
    /*判别是否有包含的页面*/
    val inJson1 = inJson.getAsJsonObject
    var re = false
    if(inJson1.has("attributes")) {
      val attr = inJson1.getAsJsonObject("attributes")
      re = if(attr.has("label") && attr.get("label").toString=="\"Credit-1\"") true else false
    }
    re
  }
}