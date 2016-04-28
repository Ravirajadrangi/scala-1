
"""
历史拉链表spark实现, 第一次接触，以一种最粗暴有效的方式实现，
深入之后发现这种方式数据量大的情况下是不行的，不过大体的思想
是不会变的。
"""
//====================hive建表================================
hadoop04 hdfs dfs -put /home/hdfs/zipper /data/mllib/zipper
                   hdfs dfs -put /home/hdfs/zipper_new /data/mllib/zipper_new

create external table if not exists graphx.zipper(
    userid int,
    state int,
    st  string,
    et  string
)
row format delimited fields terminated by " "
location '/data/mllib/zipper';

create external table if not exists graphx.zipper_new(
    userid int,
    state int,
    st  string,
    et  string
)
row format delimited fields terminated by " "
location '/data/mllib/zipper_new';

//==================spark实现===============================
val old_data = sqlContext.sql("SELECT * FROM graphx.zipper")
var new_data = sqlContext.sql("SELECT * FROM graphx.zipper_new")



//取old_data和new_data的交集
import org.apache.spark.sql.Row

val new_data_old = new_data.rdd.map{row=> Row(row(0), row(1), "200712", row(3))}

val new_data_old_dataframe = sqlContext.createDataFrame(new_data_old, new_data.schema)

val intersect_dataframe = new_data_old_dataframe.intersect(old_data)


//取new_data-old_data(差集)
import org.apache.spark.sql.Row

val new_data_old = new_data.rdd.map{row=> Row(row(0), row(1), "200712", row(3))}

val subtract_rdd = new_data_old.subtract(old_data.rdd).map{row=> 
    Row(row(0), row(1), "200801", row(3))}

val subtract_dataframe = sqlContext.createDataFrame(subtract_rdd, new_data.schema)


//取old_data-new_data(差集)
val subtract_rdd_o = old_data.rdd.subtract(new_data_old)

//封链(修改old_data-new_data(差集)的时间)
val subtract_rdd_o_nt = subtract_rdd_o.map{row=>
    Row(row(0), row(1), row(2), "200801")
}

val subtract_rdd_o_dataframe = sqlContext.createDataFrame(subtract_rdd_o_nt, new_data.schema)


//合并三个集合
val final_dataframe = intersect_dataframe.unionAll(subtract_dataframe).unionAll(subtract_rdd_o_dataframe)


//往往hive写入
val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
import hiveContext.implicits._
hiveContext.sql("use graphx")
final_dataframe.insertInto(tableName="zipper", overwrite=true)


//================spark DataFrame 之Row========================
"""
在做这个之前总是对Row这个spark DataFrame的中的数据容器不满意，
居然不能通过定Array直接to到Row，每次只能Row(a(1), a(2), a(3)....)
查了spark scala的 API发现row 其实是可以与seq相互转换的，好吧！一切
的问题就都解决了。
"""
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}
val input = Array("de ng yo", "sh iq in")
val row_rdd = sqlContext.sparkContext.parallelize(input).map(line=> line.split(" ").toSeq).map(Row.fromSeq(_))
val schema = StructType("1,2,3".split(",").map(n=>StructField(n,StringType,true)))
val te = sqlContext.createDataFrame(row_rdd, schema)