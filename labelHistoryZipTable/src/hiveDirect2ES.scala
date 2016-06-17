import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

object hiveDirect2ES {

  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("flume_streaming1")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val odata = sqlContext.sql("SELECT * FROM uts.ulb_all_dupd_m where ymd=20160518")
  }
}
