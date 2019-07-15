import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.StdIn

class Producer(topic: String, brokers: String) {

  val producer = new KafkaProducer[String, String](configuration)

  private def configuration: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props
  }

  def sendMessages(): Unit = {
    println("Enter message (type exit to quit)")
    var message = StdIn.readLine()

    while (! message.equals("exit")) {
      val record = new ProducerRecord[String, String](topic, "1", message)
      producer.send(record)
      message = StdIn.readLine()
    }

    producer.close()
  }

}

object Producer extends App {

  val producer = new Producer(brokers = "localhost:9092", topic = "mytest")
  producer.sendMessages()

}