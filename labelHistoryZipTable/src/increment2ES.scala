
import incrementTools.zipIncre
import hive2ES.process

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, LogManager}
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

object increment2ES {
  def main(args: Array[String]) {

    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("flume_streaming1")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val odata = sqlContext.sql("SELECT * FROM uts.ulb_all_dupd_m where ymd=20160518")
    val ndata = sqlContext.sql("SELECT * FROM uts.ulb_all_dupd_m where ymd=20160526")
    val rmList = ArrayBuffer("ymd")

//    val newMobile = zipIncre.getChangedMobile(odata, ndata, rmList)
    val newMobile = zipIncre.optimalGetChangedMobile(odata, ndata, rmList)
    newMobile.cache()

    val newData = ndata.join(newMobile, newMobile("pk_mobile")===ndata("pk_mobile"),"leftsemi")  //取出变化的数据
    newMobile.unpersist()
    newData.cache()

    val fen2yuanStr = """blr_xykhk_amt_3m,blr_xykhk_amt_12m,blr_hfcz_amt_3m,blr_hfcz_amt_12m,blr_zzhk_amt_2m,blr_zzhk_amt_12m,rf_lqb_acbal,rf_lfp_acbal,rf_lkd_acbal,blf_qb_acbal_amt_lst,blf_qb_acbal_amt_max_all,blf_kd_acbal_amt_lst,blf_kd_acbal_amt_max_all,blf_kd_acin_amt_fst,blf_kd_acin_amt_lst,blf_kd_acout_amt_fst,blf_kd_acout_amt_lst,blf_lfp_acbal_amt_lst,blf_lfp_acbal_amt_max_all,blf_lfp_acin_amt_fst,blf_lfp_acin_amt_lst,blf_lfp_acout_amt_fst,blf_lfp_acout_amt_lst,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_loan_amt_lst,blf_yfq_loan_amt_lst,blf_ygd_loan_amt_lst,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt,blf_qb_acbal_amt_mavg_all,blf_qb_acbal_amt_mavg_3m,blf_qb_acbal_amt_mavg_6m,blf_qb_acbal_amt_mavg_12m,blf_qb_acbal_amt_max_3m,blf_qb_acin_amt_mavg_all,blf_qb_acin_amt_mavg_3m,blf_qb_acin_amt_mavg_6m,blf_kd_acbal_amt_mavg_all,blf_kd_acbal_amt_mavg_3m,blf_kd_acbal_amt_mavg_6m,blf_kd_acbal_amt_mavg_12m,blf_kd_acbal_amt_max_3m,blf_lfp_acbal_amt_mavg_all,blf_lfp_acbal_amt_mavg_3m,blf_lfp_acbal_amt_mavg_6m,blf_lfp_acbal_amt_mavg_12m,blf_lfp_acbal_amt_max_3m,blf_tnh_loan_amt_6m,blf_tnh_loan_amt_12m,blf_tnh_loan_max_amt_12m,blf_yfq_loan_amt_6m,blf_yfq_loan_amt_12m,blf_yfq_loan_max_amt_12m,blf_ygd_loan_amt_6m,blf_ygd_loan_amt_12m,blf_ygd_loan_max_amt_12m,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt"""
    val yuanDF = process.fen2yuan(newData, fen2yuanStr)

    val desField = "as_idc"
    val desDF = process.desensitization(yuanDF, desField)
    newData.unpersist()
    desDF.cache()
    desDF.count

    desDF.saveToEs(Map(ES_RESOURCE_WRITE -> "test/test1", ES_NODES -> "10.1.80.74",
      ES_MAPPING_ID -> "pk_mobile"))
  }
}

