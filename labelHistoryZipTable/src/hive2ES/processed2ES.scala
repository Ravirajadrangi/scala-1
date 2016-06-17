package hive2ES

/**
  * Created by yn on 2016/5/3.
  */

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{StructType,StructField,DoubleType}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Range
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.sql._

object processed2ES {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("flume_streaming1")
//            .set("es.batch.size.bytes", "300000000")
//            .set("es.batch.size.entries", "10000")
//            .set("es.batch.write.refresh", "false")
//            .set("es.batch.write.retry.count", "50")
//            .set("es.batch.write.retry.wait", "500")
//            .set("es.http.timeout", "5m")
//            .set("es.http.retries", "50")
//            .set("es.action.heart.beat.lead", "50")
        val sc = new SparkContext(conf)
        val sqlContext = new HiveContext(sc)
        val data = sqlContext.sql("SELECT * FROM uts.ulb_all_dupd_m where ymd=20160518")
        val fen2yuanStr = """blr_xykhk_amt_3m,blr_xykhk_amt_12m,blr_hfcz_amt_3m,blr_hfcz_amt_12m,blr_zzhk_amt_2m,blr_zzhk_amt_12m,rf_lqb_acbal,rf_lfp_acbal,rf_lkd_acbal,blf_qb_acbal_amt_lst,blf_qb_acbal_amt_max_all,blf_kd_acbal_amt_lst,blf_kd_acbal_amt_max_all,blf_kd_acin_amt_fst,blf_kd_acin_amt_lst,blf_kd_acout_amt_fst,blf_kd_acout_amt_lst,blf_lfp_acbal_amt_lst,blf_lfp_acbal_amt_max_all,blf_lfp_acin_amt_fst,blf_lfp_acin_amt_lst,blf_lfp_acout_amt_fst,blf_lfp_acout_amt_lst,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_loan_amt_lst,blf_yfq_loan_amt_lst,blf_ygd_loan_amt_lst,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt,blf_qb_acbal_amt_mavg_all,blf_qb_acbal_amt_mavg_3m,blf_qb_acbal_amt_mavg_6m,blf_qb_acbal_amt_mavg_12m,blf_qb_acbal_amt_max_3m,blf_qb_acin_amt_mavg_all,blf_qb_acin_amt_mavg_3m,blf_qb_acin_amt_mavg_6m,blf_kd_acbal_amt_mavg_all,blf_kd_acbal_amt_mavg_3m,blf_kd_acbal_amt_mavg_6m,blf_kd_acbal_amt_mavg_12m,blf_kd_acbal_amt_max_3m,blf_lfp_acbal_amt_mavg_all,blf_lfp_acbal_amt_mavg_3m,blf_lfp_acbal_amt_mavg_6m,blf_lfp_acbal_amt_mavg_12m,blf_lfp_acbal_amt_max_3m,blf_tnh_loan_amt_6m,blf_tnh_loan_amt_12m,blf_tnh_loan_max_amt_12m,blf_yfq_loan_amt_6m,blf_yfq_loan_amt_12m,blf_yfq_loan_max_amt_12m,blf_ygd_loan_amt_6m,blf_ygd_loan_amt_12m,blf_ygd_loan_max_amt_12m,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt"""
        val yuanDF = process.fen2yuan(data, fen2yuanStr)

        val desField = "as_idc"
        val desDF = process.desensitization(yuanDF, desField)

        desDF.saveToEs(Map(ES_RESOURCE_WRITE -> "label/ulb_collect_all", ES_NODES -> "10.1.80.75",
            ES_MAPPING_ID -> "pk_mobile"))
    }
}

object process{
    def desensitization(data:DataFrame, field:String):DataFrame={
        /*身份证后脱敏（后四位变成****）*/
        val fieldArr = data.schema.map{el=>
            el.name
        }.toArray
        val index = fieldArr.indexOf(field)
        var deData:RDD[Row] = data.rdd
        if(index != -1) {
            deData = data.rdd.map { row =>
                val newRow = row.toSeq.toBuffer
                if (newRow(index) != null) {
                    val te = newRow(index).toString.slice(0, newRow(index).toString.length-4) + "****"
                    newRow(index) = te
                }
                Row.fromSeq(newRow.toSeq)
            }
        }else{
            deData = data.rdd
        }
        val sqlContext = SQLContextSingleton.getInstance(data.rdd.sparkContext)   //data.sqlContext
        val reDF = sqlContext.createDataFrame(deData, data.schema)
        reDF
    }


    def fen2yuan(data:DataFrame, fields:String):DataFrame = {
        /*金额由分变成元*/
        val fen2yuanField = fields.split(",")
        val fieldArr = data.schema.map{el=>
            el.name
        }.toArray
        val indexF2Y:Array[Int] = fen2yuanField.map{fi=>
            fieldArr.indexOf(fi)
        }.filter{el=>
            if (el == -1) false else true
        }.toSet.toArray
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
                if(newRow(i)!=null) {
                    val te = newRow(i).toString.toDouble / 100
                    newRow(i) = te
                }
            }
            Row.fromSeq(newRow.toSeq)
        }
        val sqlContext = SQLContextSingleton.getInstance(data.rdd.sparkContext)   //data.sqlContext
        val yuanDataFrame = sqlContext.createDataFrame(yuanData, mySchema)
        yuanDataFrame
    }
}

object SQLContextSingleton{
    /*sqlContext的单例*/
    @transient private var instance:SQLContext = _

    def getInstance(sparkContext: SparkContext):SQLContext = {
        if(instance == null){
            instance = new SQLContext(sparkContext)
        }
        instance
    }
}