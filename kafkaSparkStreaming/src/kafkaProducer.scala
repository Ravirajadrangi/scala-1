

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig,ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object kafkaProducer {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: kafkaProducer <filePath> <metadataBrokerList> <topic>")
      System.exit(1)
    }


    val Array(filePath, brokers, topic, _*) = args
//    val filePath = "/home/dyh/data/sample_20160516_mobile-access-log_02"
//    val brokers = "10.1.80.68:9092,10.1.80.60:9092"
//    val topic = "mobile1"

    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    Source.fromFile(filePath).getLines.foreach{line=>
      val record = new ProducerRecord[String, String](topic.toString, line.toString)
      producer.send(record)
      TimeUnit.MILLISECONDS.sleep(1)
    }

  }
}
