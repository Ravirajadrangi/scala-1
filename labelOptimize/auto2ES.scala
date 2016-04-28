/*
@author: bill_cpp
一个自动将hive中的数据灌入ES的小程序
*/


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import scala.util.Random
object sq {
  def main(args: Array[String]) {
    if(args.length != 2){
      println("please provide 1 parameters <es_index> <table_name> eg label uts.ulb_rt_m")
      System.exit(1)
    }
    val indexName:String = args(0)
    val tableName:String = args(1)



    val conf = new SparkConf().setAppName("flume_streaming1")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val sql_s:String = "SELECT * FROM %s".format(tableName)
    val typeName:String = tableName.split("\\.")(1)
    val routeEs:String = indexName + "/" + typeName
    val es_host = choiceHost()
    val data = sqlContext.sql(sql_s)
    data.saveToEs(Map(ES_RESOURCE_WRITE->routeEs,ES_NODES->es_host,
      ES_MAPPING_ID->"pk_mobile"))
  }

  def choiceHost():String={
    /**
      * 随机选择一个Elastic search的ip
      *
      */
    val r = new Random(2)
    val b = r.nextBoolean()
    if(b){"10.1.60.132"}
    else{"10.1.60.133"}
  }
}