import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import yauza.benchmark.common.{Config, Product}

import scala.collection.mutable.ArrayBuffer

object KafkaChecker {

  def main(args: Array[String]): Unit = {
    val config:Config = new Config(Config.CONFIF_FILE_NAME)
    val kafkaProps = config.getKafkaProperties()

    new Consumer(config.getProperty(Config.INPUT_TOPIC_PROP_NAME, Config.INPUT_TOPIC_NAME), kafkaProps).run()
  }

  class Consumer (val topic: String, val props:Properties) {
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "yauza-checker"
      + scala.util.Random.nextInt(1000).toString)

    // kafka 8
    props.put("session.timeout.ms", "30000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest")
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "range")
    props.put("zookeeper.connect", "localhost:2181")

    //  val consumer:KafkaConsumer[Integer, String] = new KafkaConsumer(props);

    def run():Unit = {
      var result = ArrayBuffer[Product]()

      // Kafka 8
      val consumerStream = kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(props))
      val topicCountMap = new util.HashMap[String, Integer]()
      topicCountMap.put(topic, new Integer(1))
      val consumerMap = consumerStream.createMessageStreams(topicCountMap)

      new Thread(new Runnable() {
        override def run(): Unit = {
          Thread sleep 10000
          consumerStream.shutdown()
        }
      }).start

      val stream = consumerMap.get(topic).get(0)
      val it = stream.iterator()

      var messageExample = ""
      var counter:Long = 0
      var beginTime = System.currentTimeMillis()
      while (it.hasNext()) {
        val message = new String(it.next().message())
        if (counter == 0) messageExample = message
        counter += 1
      }
      val processingTime = System.currentTimeMillis() - beginTime
      System.out.println(
        s"""
           |*******************************************************
           |Kafka topic: $topic
           |Message example:
           |$messageExample
           |
           |Processed $counter messages
           |Processing time ${processingTime} ms
           |Performance ${(counter * 1000)/processingTime} messages per sec
           |*******************************************************
           |""".stripMargin)
    }
  }
}
