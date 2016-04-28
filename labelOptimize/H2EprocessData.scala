/*
@author:bill_cpp
由于spark集群相对于ES集群要大很多，如果spark全开的话ES会受不住、报错，
所以设置了一spark向ES中写的参数，其实直接设置一spark num-executors就会解决这个
问题
另外，从数据处理开始这一段是为使hive的数据到ES之前规范化，比如金额从分到元，主要
就是对spark DataFrame中的各种数据类型的转换之类乱七八糟的东西。
由于自已比较懒，这不是一个可执行工程。
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import scala.util.Random

object abc {
  def main(args: Array[String]) {
    if(args.length != 2){
      println("please provide 1 parameters <es_index> <table_name> eg label uts.ulb_rt_m")
      System.exit(1)
    }
    val indexName:String = args(0)
    val tableName:String = args(1)



    val conf = new SparkConf().setAppName("flume_streaming1")
      .set("es.batch.size.bytes", "300000000")
      .set("es.batch.size.entries", "10000")
      .set("es.batch.write.refresh", "false")
      .set("es.batch.write.retry.count", "50")
      .set("es.batch.write.retry.wait", "500")
      .set("es.http.timeout", "5m")
      .set("es.http.retries", "50")
      .set("es.action.heart.beat.lead", "50")

    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    //================数据处理开始======================
    import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}
    import org.elasticsearch.spark.sql._
    import org.apache.spark.sql.Row
    import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
    val data = sqlContext.sql("SELECT * FROM UTS.ulb_collect_all_sample")
    val fen2yuanStr = """rf_lqb_acbal,rf_lfp_acbal,rf_lkd_acbal,blf_qb_acbal_amt_lst,blf_qb_acbal_amt_max_all,blf_kd_acbal_amt_lst,blf_kd_acbal_amt_max_all,blf_kd_acin_amt_fst,blf_kd_acin_amt_lst,blf_kd_acout_amt_fst,blf_kd_acout_amt_lst,blf_lfp_acbal_amt_lst,blf_lfp_acbal_amt_max_all,blf_lfp_acin_amt_fst,blf_lfp_acin_amt_lst,blf_lfp_acout_amt_fst,blf_lfp_acout_amt_lst,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_loan_amt_lst,blf_yfq_loan_amt_lst,blf_ygd_loan_amt_lst,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt,blf_qb_acbal_amt_mavg_all,blf_qb_acbal_amt_mavg_3m,blf_qb_acbal_amt_mavg_6m,blf_qb_acbal_amt_mavg_12m,blf_qb_acbal_amt_max_3m,blf_qb_acin_amt_mavg_all,blf_qb_acin_amt_mavg_3m,blf_qb_acin_amt_mavg_6m,blf_kd_acbal_amt_mavg_all,blf_kd_acbal_amt_mavg_3m,blf_kd_acbal_amt_mavg_6m,blf_kd_acbal_amt_mavg_12m,blf_kd_acbal_amt_max_3m,blf_lfp_acbal_amt_mavg_all,blf_lfp_acbal_amt_mavg_3m,blf_lfp_acbal_amt_mavg_6m,blf_lfp_acbal_amt_mavg_12m,blf_lfp_acbal_amt_max_3m,blf_tnh_loan_amt_6m,blf_tnh_loan_amt_12m,blf_tnh_loan_max_amt_12m,blf_yfq_loan_amt_6m,blf_yfq_loan_amt_12m,blf_yfq_loan_max_amt_12m,blf_ygd_loan_amt_6m,blf_ygd_loan_amt_12m,blf_ygd_loan_max_amt_12m,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt"""
    val fen2yuanField = fen2yuanStr.split(",")
    val fieldArr = data.schema.map{el=>
//        case StructField(fieldName:String, myType:StringType, bool:Boolean) => fieldName.toString
        el.name
    }.toArray
    val indexF2Y:Array[Int] = fen2yuanField.map{fi=>
        fieldArr.indexOf(fi)
    }.filter{el=>
        if (el == -1) false else true
    }.toSet.toArray

    import scala.collection.immutable.Range
    val mySchemaVec = Range(0, data.schema.length).map{i=>
        if(indexF2Y.contains(i)) {
            StructField(data.schema(i).name, DoubleType, true)
        }else{
            data.schema(i)
        }
    }
    val mySchema = StructType(mySchemaVec)

      val yuanData = data.rdd.map{row=>
          val newRow = row.toSeq.toBuffer
          for(i<-indexF2Y){
              if(newRow(i).toString!="not applied") {
                  val te = newRow(i).toString.toDouble / 100
                  newRow(i) = te
              }
          }
          //        if(newRow(1)==null) newRow(1) = "NA"
          Row.fromSeq(newRow.toSeq)
      }.repartition(25).cache

    val yuanDataFrame = sqlContext.createDataFrame(yuanData, mySchema)

    yuanDataFrame.saveToEs(Map(ES_RESOURCE_WRITE->"label_1/ulb_collect_all",ES_NODES->"10.1.60.132",
      ES_MAPPING_ID->"pk_mobile"))

    //================数据处理结束======================



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
