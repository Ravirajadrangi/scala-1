/*
@author:bill_cpp
自定义partitioner，将相同手机号的历史数据和新表数据
发送到相同的partition中,然后将各个partition中历史数据
hash值做成set，判别每一条数据是否发生变化。
*/


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.Partitioner
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._


object historyZip {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("flume_streaming1")
        val sc = new SparkContext(conf)
        val sqlContext = new HiveContext(sc)

        val oldDataFrame1 = sqlContext.sql("select * from uts.ulb_collect_all")
//        val dropNames = Array("st", "et")
//        val oldDataFrame1 = dropNames.foldLeft(oldDataFrame) {
//            //drop掉"st","et"两列
//            case (d: DataFrame, name: String) => d.drop(name)
//        }
        val newDataFrame = sqlContext.sql("select * from graphx.ulb_new_all")
        val oldData = oldDataFrame1.rdd.map{row=>
            var t_seq = row.toSeq.toBuffer
            t_seq += 0
            Row.fromSeq(t_seq.toSeq)
        }
        val newData = newDataFrame.rdd.map{row=>
            var t_seq = row.toSeq.toBuffer
            t_seq += 1
            Row.fromSeq(t_seq.toSeq)
        }

        val ulbUnion = oldData.union(newData).map{row=>
            (row(0), row)
        }

        val parUlb = ulbUnion.partitionBy(new dyhPartitioner(1000))

//        parUlb.mapPartitions()
        val onlyNew = parUlb.mapPartitions{ iter=>
            val iter1 = iter.toArray
            val old_r = iter1.iterator.filter{i=>i._2(i._2.length-1)==0}.map{i=>
                val tmp = i._2.toSeq.toBuffer
                Row.fromSeq(tmp.slice(0, tmp.length-2).toSeq)
            }
            val new_r = iter1.iterator.filter{i=>i._2(i._2.length-1)==0}.map{i=>
                val tmp = i._2.toSeq.toBuffer
                Row.fromSeq(tmp.slice(0, tmp.length-2).toSeq)
            }
            val n_o_r = new_r.toSet.diff(old_r.toSet)
            // val n_o_r = new_r.toSet.diff(Set(("a",8)))
            n_o_r.iterator
        }

        val toES = sqlContext.createDataFrame(onlyNew, newDataFrame.schema)
        // te.insertInto(tableName="graphx.ulb_new", overwrite=true)
        toES.saveToEs(Map(ES_RESOURCE_WRITE->"test1/ulb_test",ES_NODES->"10.1.60.132",
            ES_MAPPING_ID->"pk_mobile"))

    }
}


class dyhPartitioner(numParts:Int) extends Partitioner{
    /*"""partitioner的策略: hashCode(手机号)%numParts"""*/

    override def numPartitions: Int = numParts

    override def getPartition(key:Any):Int = {
        val tmpHash = key.hashCode
        val parNum = tmpHash % numPartitions
        if(parNum<0){
            parNum + numPartitions
        }else{
            parNum
        }
    }
}