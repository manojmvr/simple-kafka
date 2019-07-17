import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.StdIn

class Producer(topic: String, brokers: String) {

  var count = 0

  val producer = new KafkaProducer[String, String](configuration)

  private def configuration: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props
  }

  def sendMessages(): Unit = {
    println("Sending message to all the paritions in round robin fashion every 1s")

    var message = "hi ";
    while (! message.equals("exit")) {
      // Skip key to produce message in a round robin fashion
      val record = new ProducerRecord[String, String](topic, s"${count}")
      producer.send(record)
      Thread.sleep(1000)
      count = count + 1
    }

    producer.close()
  }

}

object Producer extends App {

  val producer = new Producer(brokers = "localhost:9092", topic = "test3")
  producer.sendMessages()

}