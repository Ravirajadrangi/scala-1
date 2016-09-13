package realTimeSimulation.readDataFromHive



import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
  * Spark streaming从Hive中读取数据，并返回DataFrame
  */
object getDataFrame {

  def getDF(ssc: StreamingContext, hiveSql:String): DataFrame ={
    val sqlContext = new SQLContext(ssc.sparkContext)
    val DF = sqlContext.sql(hiveSql.toString)
    DF
  }
}
