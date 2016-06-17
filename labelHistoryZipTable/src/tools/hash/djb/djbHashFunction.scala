package tools.hash.djb

/**
  * Created by yn on 2016/6/12.
  */
class djbHashFunction {
  def DJB_HASH(value:String): Int ={
    var hash:Long = 5381
    for(i<- 0 until value.length){
      hash = ((hash<<5) + hash) + value.charAt(i)
    }
    hash.toInt
  }
}
