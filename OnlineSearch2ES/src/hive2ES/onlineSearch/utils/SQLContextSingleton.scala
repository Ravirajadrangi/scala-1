package hive2ES.onlineSearch.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by yn on 2016/9/12.
  */
object SQLContextSingleton {
  /*sqlContext的单例*/
  @transient private var instance:SQLContext = _

  def getInstance(sparkContext: SparkContext):SQLContext = {
    if(instance == null){
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
