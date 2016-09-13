package realTimeSimulation.filterStreaming

import org.apache.spark.sql.Row


/**
  * 清洗出需要的四个字段，它们的位置分别是6,8,12,24(从0开始计算)
  */
object filterStreaming1 {

  def getFields(line:String, sites:Array[Int]): Row={
    val fields = line.stripLineEnd.split("\001")
    val re = sites.map(fields(_))
    Row.fromSeq(re.toSeq)
  }

  def filterCardnoNull(r:Row, CardnoSite:Int): Boolean={
    if(Row(CardnoSite).toString()==null) false else true
  }
}
