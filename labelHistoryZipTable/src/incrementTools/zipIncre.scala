package incrementTools


import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType,StringType,StructField}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer


object zipIncre {
  def optimalGetChangedMobile(oldData:DataFrame, newData:DataFrame,rmCol:ArrayBuffer[String]):DataFrame={
    /*找出数据中发生变化的行数的手机号
    * @oldData:历史数据
    * @newData:新数据
    * @rmCol:不参与计算的列名*/
//    val drop = (d:DataFrame, r:ArrayBuffer[String]) => r.foldLeft(d){  //drop消rmCol中的列
//      case(i:DataFrame, name:String) => i.drop(name)
//    }
//    val oData = drop(oldData, rmCol)
//    val nData = drop(newData, rmCol)
//    oData.cache()
//    nData.cache()
//    oData.count()
//    nData.count()

    val mobHash = (d:DataFrame) => d.map{row=>   //生成Row(pk_mobile,hashCode)
      val tmp = row.toSeq
      val hc = tmp.slice(0, row.toSeq.length-1).mkString("\001").hashCode
      val t_seq = ArrayBuffer(row(0), hc)
      Row.fromSeq(t_seq.toSeq)
    }
    val old_data = mobHash(oldData)
    val new_data = mobHash(newData)
    old_data.cache()
    new_data.cache()
    val sqlContext = oldData.sqlContext
    val createDF = (d:RDD[Row], s:StructType) => sqlContext.createDataFrame(d,s)   //生成DataFrame

    val mob = new_data.subtract(old_data).map(row=>Row(row(0)))
    old_data.unpersist()
    new_data.unpersist()
    val schema = StructType(Array(StructField("pk_mobile",StringType,true)))
    createDF(mob, schema)
  }


  def getChangedMobile(oldData:DataFrame, newData:DataFrame,rmCol:ArrayBuffer[String]):DataFrame={
    /*找出数据中发生变化的行数的手机号
    * @oldData:历史数据
    * @newData:新数据
    * @rmCol:不参与计算的列名*/
    val drop = (d:DataFrame, r:ArrayBuffer[String]) => r.foldLeft(d){  //drop消rmCol中的列
      case(i:DataFrame, name:String) => i.drop(name)
    }
    val oData = drop(oldData, rmCol)
    val nData = drop(newData, rmCol)

    val mobHash = (d:DataFrame) => d.map{row=>   //生成Row(pk_mobile,hashCode)
      val hc = row.toSeq.mkString("\001").hashCode
      val t_seq = ArrayBuffer(row(0), hc)
      Row.fromSeq(t_seq.toSeq)
    }
    val old_data = mobHash(oData)
    val new_data = mobHash(nData)
    old_data.cache()
    new_data.cache()

    val sqlContext = oldData.sqlContext
//    val schema = StructType(Array(StructField("pk_mobile",StringType,true), StructField("num", StringType, true)))
    val createDF = (d:RDD[Row], s:StructType) => sqlContext.createDataFrame(d,s)   //生成DataFrame
//    val oDF = createDF(old_data, schema)
//    val nDF = createDF(new_data, schema)

    val mob = new_data.subtract(old_data).map(row=>Row(row(0)))
    val schema = StructType(Array(StructField("pk_mobile",StringType,true)))
    createDF(mob, schema)
  }
}

