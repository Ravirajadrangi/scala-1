package surgicalStrikeSpark2ES

import hive2ES.process
import org.apache.log4j.{Level, LogManager}
import org.elasticsearch.spark.myEsRDDWriter.EsPartitionDFWriter

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.apache.spark.TaskContext

object surgicalStrike {
  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("surgicalStrike")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val settings = new SparkSettingsManager().load(sqlContext.sparkContext.getConf).copy()
    settings.setNodes("10.1.80.75")
    settings.setResourceRead("test/test1")
    settings.setResourceWrite("test/test1")
    val settingsStr = settings.save()

    val data = sqlContext.sql("SELECT * FROM graphx.ulb_collect_sample_20160611 where ymd=20160611")
    val fen2yuanStr = """blr_xykhk_amt_3m,blr_xykhk_amt_12m,blr_hfcz_amt_3m,blr_hfcz_amt_12m,blr_zzhk_amt_2m,blr_zzhk_amt_12m,rf_lqb_acbal,rf_lfp_acbal,rf_lkd_acbal,blf_qb_acbal_amt_lst,blf_qb_acbal_amt_max_all,blf_kd_acbal_amt_lst,blf_kd_acbal_amt_max_all,blf_kd_acin_amt_fst,blf_kd_acin_amt_lst,blf_kd_acout_amt_fst,blf_kd_acout_amt_lst,blf_lfp_acbal_amt_lst,blf_lfp_acbal_amt_max_all,blf_lfp_acin_amt_fst,blf_lfp_acin_amt_lst,blf_lfp_acout_amt_fst,blf_lfp_acout_amt_lst,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_loan_amt_lst,blf_yfq_loan_amt_lst,blf_ygd_loan_amt_lst,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt,blf_qb_acbal_amt_mavg_all,blf_qb_acbal_amt_mavg_3m,blf_qb_acbal_amt_mavg_6m,blf_qb_acbal_amt_mavg_12m,blf_qb_acbal_amt_max_3m,blf_qb_acin_amt_mavg_all,blf_qb_acin_amt_mavg_3m,blf_qb_acin_amt_mavg_6m,blf_kd_acbal_amt_mavg_all,blf_kd_acbal_amt_mavg_3m,blf_kd_acbal_amt_mavg_6m,blf_kd_acbal_amt_mavg_12m,blf_kd_acbal_amt_max_3m,blf_lfp_acbal_amt_mavg_all,blf_lfp_acbal_amt_mavg_3m,blf_lfp_acbal_amt_mavg_6m,blf_lfp_acbal_amt_mavg_12m,blf_lfp_acbal_amt_max_3m,blf_tnh_loan_amt_6m,blf_tnh_loan_amt_12m,blf_tnh_loan_max_amt_12m,blf_yfq_loan_amt_6m,blf_yfq_loan_amt_12m,blf_yfq_loan_max_amt_12m,blf_ygd_loan_amt_6m,blf_ygd_loan_amt_12m,blf_ygd_loan_max_amt_12m,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt"""
    val yuanDF = process.fen2yuan(data, fen2yuanStr)

    val desField = "as_idc"
    val desDF = process.desensitization(yuanDF, desField)

    val schema = desDF.schema
    val bcVal = sc.broadcast(schema)

    desDF.rdd.map { row => (row(0), row) }.partitionBy(new ESShardPartitionerLessConcurrent(settingsStr)).foreachPartition {
      iter =>
        try {
          val newSettings = new PropertiesSettings().load(settingsStr)
          //create EsRDDWriter
            val schema = bcVal.value
            val writer = new EsPartitionDFWriter(schema, newSettings.save())
            writer.write(TaskContext.get(), iter.map(f => f._2))
        }
    }
  }
}
