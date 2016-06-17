package surgicalStrikeSpark2ES

import tools.hash.djb.djbHashFunction

import org.apache.spark.Partitioner
import org.apache.commons.logging.LogFactory
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.RestRepository

class ESShardPartitionerLessConcurrent(settings:String) extends Partitioner{
  protected val log = LogFactory.getLog(this.getClass())

  protected var _numPartitions = -1
  override def numPartitions:Int = {
    val newSettings = new PropertiesSettings().load(settings)
    val repository =new RestRepository(newSettings)
    val targetShards = repository.getWriteTargetPrimaryShards(newSettings.getNodesClientOnly())
//    val targetShards = repository.getWriteTargetPrimaryShards(true)
    repository.close()
    _numPartitions = targetShards.size()
    _numPartitions
  }

  override def getPartition(key: Any): Int = {
    val shardObject = new ShardAlg()
    val shardId = shardObject.shard(key.toString(), _numPartitions)
    shardId
  }
}

class ShardAlg{
  def shard(id:String, shardNum:Int): Int={
    val djb = new djbHashFunction()
    val hash = djb.DJB_HASH(id)
    mod(hash, shardNum)
  }

  def mod(v:Int, m:Int): Int={
    var r = v % m
    if(r < 0){
      r += m
    }
    r
  }
}

