import org.apache.spark.{SparkContext, SparkConf}

object syntaxTest {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("flume_streaming1")
        val sc = new SparkContext(conf)

        val input = sc.textFile("/data/mllib/adult_small.txt")
        val output = input.map(l=>myFunction.func1(l))
        println(output.first+"+++++++++++++++++++++++++++++++++++++++++++++++")
    }
}

object myFunction{
    def func1(s:String):String = {
        s.length.toString
    }
}