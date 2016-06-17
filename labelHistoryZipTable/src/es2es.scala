
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext



object es2es {
  val conf = new SparkConf().setAppName("flume_streaming1")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val options = Map("pushdown"->"true", "es.nodes"->"10.1.80.75", "es.port"->"9200")
  val sparkDF = sqlContext.read.format("org.elasticsearch.spark.sql").options(options).load("streaming/test")
}
