/*
  @author: bill_cpp
  @spark: 1.5.0
  下面的代码记录的是数据从hive到spark，在spark做完处理后进入
  elastic search中的过程，为了方便测试，代码都是直接在spark-shell中的执行，完
  整的工程私我。
*/

//=======将lily生成的ulb_collect_all_sample表加上时间字段===========
val old_data = sqlContext.sql("select * from uts.ulb_collect_all_sample")

import org.apache.spark.sql.Row
val tmp:Seq[String] = Seq("20160421", "29990909")
val date_old_data_rdd = old_data.rdd.map{ row=>
    var n_row = Row.fromSeq(row.toSeq ++ tmp)    //第一个Row上添加两列
    n_row
}

import org.apache.spark.sql.types.{StructType,StructField,StringType}
val date_schema_buffer = old_data.schema.toBuffer ++ Array(StructField("st", StringType, true),
       StructField("et", StringType, true))
val date_schema = StructType(date_schema_buffer)  //更新schema
val final_dataframe = sqlContext.createDataFrame(date_old_data_rdd, date_schema)
final_dataframe.cache


val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
import hiveContext.implicits._
hiveContext.sql("use graphx")
final_dataframe.insertInto(tableName="ulb_old", overwrite=true)  //写入hive


//============================================================
"""
更新10000条数据，随机选10000条数据，将an_age更新为-100.
应用拉链，加入到ES中去。
"""
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

sqlContext.sql("create table graphx.ulb_new like uts.ulb_collect_all_sample")
val old_data = sqlContext.sql("select * from graphx.ulb_old")
val drop_names = Array("st", "et")
val new_data = drop_names.foldLeft(old_data){  //drop掉"st","et"两列
    case(d:DataFrame, name:String) => d.drop(name)
}
// new_data.repartition(100).cache

val c_new_data = new_data.count
val pro_change = 10000.toDouble/c_new_data
val Array(change_new, unchange_new) = new_data.randomSplit(Array(pro_change, 1-pro_change))
val new_data1 = change_new.rdd.map{row=>   //随机取出10000条数据将an_age变成-100
    var t_seq = row.toSeq.toBuffer
    t_seq(3) = -100
    Row.fromSeq(t_seq.toSeq)
}.union(unchange_new.rdd)

val te = sqlContext.createDataFrame(new_data1, new_data.schema)
// te.insertInto(tableName="graphx.ulb_new", overwrite=true)
te.saveToEs(Map(ES_RESOURCE_WRITE->"test/ulb_test",ES_NODES->"10.1.60.132",
  ES_MAPPING_ID->"pk_mobile"))
.union(unchange_new.rdd)

// new_data1.repartition(100).cache

// new_data.saveToEs(Map(ES_RESOURCE_WRITE->"test/ulb_test",ES_NODES->"10.1.60.132",
//   ES_MAPPING_ID->"pk_mobile"))  //将旧的数据全部加载入ES中

val insert_rdd = new_data1.subtract(new_data.rdd).repartition(100)
val insert_dataframe = sqlContext.createDataFrame(insert_rdd, new_data.schema)
insert_dataframe.saveToEs(Map(ES_RESOURCE_WRITE->"test/ulb_test",ES_NODES->"10.1.60.132",
  ES_MAPPING_ID->"pk_mobile"))  //将新的数据全部加载入ES中

//================================================================================
"""
想以手机号作为partitioner的key，将历史表和新表中相同的手机号发送到相同的partition中去
然后在相同的partition中做subtract操作，以下是代码是前期的一个测试。
在这一步中定义了自己的partitioner，写在另一个工程中，参见historyZip工程
"""
val input = List(("a", 8, 0), ("c", 7, 0), ("a", 8, 1), ("c", 8, 1))
val te = sc.makeRDD(input)
val new_f = te.repartition(1).mapPartitions{iter=>
    val iter1 = iter.toArray
    val old_r = iter1.iterator.filter{_._3==0}.map{i=>(i._1, i._2)}
    val new_r = iter1.iterator.filter{_._3==1}.map{i=>(i._1, i._2)}
    val n_o_r = new_r.toSet.diff(old_r.toSet)
    // val n_o_r = new_r.toSet.diff(Set(("a",8)))
    n_o_r.iterator
    // old_r
}
new_f.collect.foreach(println(_))


//==================全量数据做历史拉链表=====================
import org.apache.spark.sql.Row

sqlContext.sql("create table graphx.ulb_new_all like uts.ulb_collect_all")

val old_data = sqlContext.sql("select * from uts.ulb_collect_all")
val Array(change_new, unchange_new) = old_data.randomSplit(Array(0.1, 0.9))

val new_data1 = change_new.rdd.map{row=>   //随机取出0.1数据（约6百万条）将an_age变成-100
    var t_seq = row.toSeq.toBuffer
    t_seq(3) = -100
    Row.fromSeq(t_seq.toSeq)
}
val new_data = unchange_new.rdd.union(new_data1)
val te = sqlContext.createDataFrame(new_data, old_data.schema)
te.insertInto(tableName="graphx.ulb_new_all", overwrite=true)


//==================20万数据添加hashCode=======================
"""
把历史表中所有的字段求hashCode，然后将所有的hashCode加入set中，并把
该set做成spark 的broadcast变量，理论上这么做的会让时间复杂度降到n，下面
的代码是抽取的20万样本数据做测试。
测试结果表明这么做让拉链的操作降到秒级别，但是会存在hash冲突的问题，比如
总量200016的数据有8条冲突。
"""
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer

// sqlContext.sql("create table graphx.ulb_old_hash like uts.ulb_collect_all")
// sqlContext.sql("alter table graphx.ulb_old_hash add columns(hashcode int)")

val old_data1 = sqlContext.sql("select * from graphx.ulb_old")
val drop_names = Array("st", "et")
val old_data = drop_names.foldLeft(old_data1){  //drop掉"st","et"两列
    case(d:DataFrame, name:String) => d.drop(name)
}

val new_data1 = old_data.rdd.map{row=>   //随机取出10000条数据将an_age变成-100
    var t_seq = row.toSeq.toBuffer
    t_seq += t_seq.mkString("\001").hashCode
    Row.fromSeq(t_seq.toSeq)
}

val old_hash_schema = sqlContext.sql("select * from graphx.ulb_old_hash").schema
val te = sqlContext.createDataFrame(new_data1, old_hash_schema)
te.insertInto(tableName="graphx.ulb_old_hash", overwrite=true)

//hash 历史拉链
val hash_key_df = sqlContext.sql("select hashcode from graphx.ulb_old_hash")
val hash_set = hash_key_df.rdd.toArray.map{i=>i(0)}.toSet
val bc_val = sc.broadcast(hash_set)

val new_data = sqlContext.sql("select * from graphx.ulb_new")
val only_data = new_data.mapPartitions{ iter=>
    var tmp:ArrayBuffer[Row] = new ArrayBuffer()
    val hSet:Set[Any] = bc_val.value
    while(iter.hasNext){
        val tmp_row = iter.next
        val tmp_hash = tmp_row.toSeq.mkString("\001").hashCode
        if (!hSet.contains(tmp_hash)){
            tmp += tmp_row
        }
    }
    tmp.iterator
}




//==================全量数据添加hashCode=======================
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer

// sqlContext.sql("create table graphx.ulb_old_all_hash like uts.ulb_collect_all")
// sqlContext.sql("alter table graphx.ulb_old_all_hash add columns(hashcode int)")

val old_data = sqlContext.sql("select * from uts.ulb_collect_all")
val new_data1 = old_data.rdd.map{row=>   
    var t_seq = row.toSeq.toBuffer
    t_seq += t_seq.mkString("\001").hashCode
    Row.fromSeq(t_seq.toSeq)
}
val all_hash_schema = sqlContext.sql("select * from graphx.ulb_old_all_hash").schema
val new_data = sqlContext.createDataFrame(new_data1, all_hash_schema)
new_data.insertInto(tableName="graphx.ulb_old_all_hash", overwrite=true)

//groupbyKey
"""
将历史表中的hashCode全量的加入内存并做成broadcast变量，但是当数据量大的时候报OOM异常，
结合之前自定义partitioner的优化步骤，将历史表(key, Row(hashCode))与新表(key, row)合并，
然后groupByKey，之后比较每个key中的value。如果row中的值hash后，与另一个hash值不相等则
表示发生变化。
"""
val hash_key_df = sqlContext.sql("select pk_mobile,hashcode from graphx.ulb_old_all_hash")
val hash_old = hash_key_df.rdd.map{case Row(key,value)=>(key, Row(value))}


val new_data = sqlContext.sql("select * from graphx.ulb_new_all")
val hash_new = new_data.rdd.map{row=> (row(0), row)}

val union_hash = hash_new.union(hash_old)
val group_hash = union_hash.groupByKey(1000)

val to_es_data = group_hash.mapPartitions{iter=>
    var tmp:ArrayBuffer[Row] = new ArrayBuffer()
    while(iter.hasNext){
        val tmp_a = iter.next
        val tmp_array = Array(tmp_a._1, tmp_a._2)
        val tmp_hash = tmp_array.map{i=>
            i match{
                case (s:String, r:Row) if(r.length==1) => r(1)
                case (s:String, c:Row) => c.toSeq.mkString("\001").hashCode  //求新表的hashCode
                case _ => 0
            }
        }
        if(tmp_hash(0)!=tmp_hash(1)){
            var tmp_f = tmp_array collect {
                    case (s:String, r:Row) => r   //做filter，并类型匹配
                }
            tmp += tmp_f(0)
        }
    }
    tmp.iterator  
}



//==================全量新数据添加hashCode=======================
"""
上面的方法虽然可以跑通，但是所需要的时间是难以接受的（大概一小时），最后
将新表和旧表都求hash，每个hash都加上对应手机号（基本可解决hash冲突），然后
做subtract，之后做join（join的顺序很重要的），起100个executors 23台的spark
集群 五分钟左右搞定6千万条 552个字段的拉链。
"""
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer

// sqlContext.sql("create table graphx.ulb_new_all_hash like graphx.ulb_new_all")
// sqlContext.sql("alter table graphx.ulb_new_all_hash add columns(hashcode int)")

val new_data1 = sqlContext.sql("select * from graphx.ulb_new_all")


val new_data = new_data1.rdd.map{row=>   
    var t_seq = row.toSeq.toBuffer
    t_seq += t_seq.mkString("\001").hashCode
    Row.fromSeq(t_seq.toSeq)
}

val new_hash_schema = sqlContext.sql("select * from graphx.ulb_new_all_hash").schema
val te = sqlContext.createDataFrame(new_data, new_hash_schema)
te.insertInto(tableName="graphx.ulb_new_all_hash", overwrite=true)

//hash 历史拉链
val hash_key_old = sqlContext.sql("select pk_mobile,hashcode from graphx.ulb_old_all_hash").rdd
val hash_key_new = sqlContext.sql("select pk_mobile, hashCode from graphx.ulb_new_all_hash").rdd

val only_new = hash_key_new.subtract(hash_key_old).map{row=> Row(row(0))}
val only_new_schema = sqlContext.sql("select pk_mobile from graphx.ulb_old_all_hash").schema
val only_new_df = sqlContext.createDataFrame(only_new, only_new_schema)
only_new_df.cache

val new_all_df = sqlContext.sql("select * from graphx.ulb_new_all")

val new_join = only_new_df.join(new_all_df, only_new_df("pk_mobile")===new_all_df("pk_mobile"))



/**/