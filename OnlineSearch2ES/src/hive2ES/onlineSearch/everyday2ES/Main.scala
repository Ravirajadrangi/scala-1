package hive2ES.onlineSearch.everyday2ES


import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dyh on 2016/8/26.
  */
object Main {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println(
        """Usage:<HiveTable> <ESIndex> <Flag> <Partitions>
          |@HiveTable: 表名,如yxt.cp_trans_success_tele
          |@ESIndex: 索引名,如yxt/cp_trans_success_tele
          |@Flag: 标记，取值为 all(全量)、special（指定分区名）、increment（指定起始分区名到当前分区）
          |@Partitions: 当Flag为all时Partitions为空；当Flag为special时Partitions为指定分区名并用","分割，
          |   如20160708,20160712表示会灌入2016-07-08和2016-07-12这两个分区的数据；
          |   当Flag为increment时Partitions为起始分区名，如20160706表示会灌入2016-07-06后的所有分区。
        """.stripMargin)  //
      //spark-submit --master yarn --name hive2es --class hiveDirect2ES --num-executors 10 --jars /home/dyh/softwares/elasticsearch-hadoop-2.3.1.jar labelHistoryZipTable.jar
      //yxt.cp_trans_success_tele
      System.exit(1)
    }

    val Array(hiveTable, esIndex, flag, _*) = args     //val args = Array("yxt.cp_trans_success_tele",  "yxt/cp_trans_success_tele","20160627")
    val specialOrIncrement:String = if(args.length>=4) args(3) else ""

    LogManager.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("yxt.cp_trans_success_tele")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val s:String = s"SELECT * FROM $hiveTable"

    val df:DataFrame = sqlContext.sql(s)

    val partitionName = OnlineSearch2ES.getPartitionName(df, hiveTable)

    flag match {
      case "all" => OnlineSearch2ES.all2ES(df, partitionName, esIndex)
      case "special" => OnlineSearch2ES.special2ES(df, partitionName, specialOrIncrement, esIndex)
      case "increment" => OnlineSearch2ES.increment2ES(df, partitionName, specialOrIncrement, esIndex)
      case _ => println("flag must be one of (all, special, increment)")
    }

  }
}
