package org.elasticsearch.spark.myEsRDDWriter

import org.elasticsearch.spark.sql.EsDataFrameWriter
import org.apache.spark.sql.types.StructType


class EsPartitionDFWriter(schema: StructType, serializedSettings:String) extends EsDataFrameWriter(schema: StructType, serializedSettings:String){

}
