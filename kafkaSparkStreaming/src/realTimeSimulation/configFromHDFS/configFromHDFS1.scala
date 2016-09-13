package realTimeSimulation.configFromHDFS

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StringType, StructField}

/*
* 从直接从hive仓库中读取数据，然后转换成DataFrame
* */
object configFromHDFS1 {
  def getConfigCardMob(sc:SparkContext, hdfsPath:String, fieldName:String):DataFrame={
    /*@fieldName中hive表的字段名用","分割*/
    val raw = sc.textFile(hdfsPath)
    val rdd = raw.map{line=>
      val arr = line.stripLineEnd.split("\001")
      Row.fromSeq(arr.toSeq)
    }

    val schemaArr = fieldName.split(",").map(field => StructField(field, StringType, true))
    val schema = StructType(schemaArr)

    val sqlContext = SQLContextSingleton.getInstance(sc)

    sqlContext.createDataFrame(rdd,schema)
  }
}


/** Lazily instantiated singletion instance of SQLContext **/
object SQLContextSingleton {
  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}