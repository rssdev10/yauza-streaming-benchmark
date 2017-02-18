package yauza.benchmark

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Properties}

import com.google.gson.Gson
import _root_.kafka.consumer.KafkaStream
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import yauza.benchmark.common.Product
import yauza.benchmark.common.Config

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.mutable.ArrayBuffer


object ResultsCollector {
  private val gson: Gson = new Gson

  @throws[Exception]
  def main(args: Array[String]) {
    val configPath = if (args.isEmpty) "config" else args(0)
    val outDir = if (args.isEmpty) Config.OUTPUT_DIR else args(1)
    val fileName = if (args.length >= 2 ) {
      args(2)
    } else {
      val now = Calendar.getInstance().getTime()
      val formatter = new SimpleDateFormat("yyyyMMdd-HHmmss")
      s"results-${formatter.format(now)}.json"
    }

    fetchResults(configPath, outDir, fileName, "localhost:9092", "localhost:2181")
  }

  def fetchResults(configPath:String, resultPath:String, filename:String, bootstrapServer:String, zookeeper:String) {
    val gson = new Gson()

    val config:Config = new Config(configPath + "/benchmark.properties")
    val kafkaProps = config.getKafkaProperties()

    val result = Array(
      "uniq-users-number",
      "uniq-sessions-number",
      "avr-price",
      "avr-session-duration"
    )
      .par
      .map(str => "out-" + str) // add common suffix
      .map(queue => {
        val array = new Consumer(queue, kafkaProps, bootstrapServer, zookeeper).run()
        array.foldLeft(new Experiment(queue)){(acc:Experiment, item:Product) => {
          if (item.getProcessedEvents > 0) {
            acc.latency.addValue(item.getLatency)
            acc.throughput.addValue((item.getProcessedEvents / (item.getProcessingTime / 1000.0)).toInt)
            acc.totalProcessed = acc.totalProcessed + item.getProcessedEvents
            acc.totalTime = acc.totalTime + item.getProcessingTime / 1000
            acc.values ++= Array(item.getValue)
          }
          acc
        }}
      })
      .filter(_.totalProcessed > 0)
      .map(experiment => {
        gson.toJson(experiment)
      }).mkString(",\n")

    try {
      new File(resultPath).mkdirs()

      val pw = new PrintWriter(new File(s"${resultPath}/${filename}"))
      pw.write(s"""{"result":[\n$result\n]}""")
      pw.close

    } catch {
      case e:Exception =>
        e.printStackTrace()
        print(result)
    }
  }

  class Consumer (val topic: String, val props:Properties, val bootstrapServer:String, val zookeeper:String) {
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "yauza"
      + scala.util.Random.nextInt(1000).toString); //debug feature to avoid seek operaion
    //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    // kafka 9, 10
//    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
//    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // kafka 8
    props.put("session.timeout.ms", "30000");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "range");
    props.put("zookeeper.connect", zookeeper);

//  val consumer:KafkaConsumer[Integer, String] = new KafkaConsumer(props);

    def run():ArrayBuffer[Product] = {
      var result = ArrayBuffer[Product]()
//      // Kafka 9
//      consumer.subscribe(Collections.singletonList(topic))
//
//      var records:ConsumerRecords[Integer, String] = consumer.poll(1000)
//      if (records.count() == 0) {
//        val partitions = consumer.assignment().toArray
//        if (partitions.length > 0) {
//          val partition = partitions(0).asInstanceOf[TopicPartition]
//          consumer.seek(partition, 0)
//          records = consumer.poll(1000)
//        }
//      }
//
//      println (s"Reading queue $topic. Found ${records.count()} messages.")
//
//      for (record:ConsumerRecord[Integer, String] <- records.iterator()) {
//        System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
//        val product = gson.fromJson(record.value(), classOf[Product])
//        result += product
//      }

      // Kafka 8
      val consumerStream = _root_.kafka.consumer.Consumer.createJavaConsumerConnector(
        new _root_.kafka.consumer.ConsumerConfig(props))
      val topicCountMap = new util.HashMap[String, Integer]()
      topicCountMap.put(topic, new Integer(1))
      val consumerMap = consumerStream.createMessageStreams(topicCountMap)

      new Thread(new Runnable() {
        override def run(): Unit = {
          Thread sleep 5000
          consumerStream.shutdown()
        }
      }).start

      for (stream: KafkaStream[Array[Byte], Array[Byte]] <- consumerMap.get(topic).iterator()) {
        val it = stream.iterator()

        while (it.hasNext()) {
          val message = new String(it.next().message())
          System.out.println(message)
          val product = gson.fromJson(message, classOf[Product])
          result += product
        }
      }

      return result
    }
  }

  class Statistics {
    var min:Long = Long.MaxValue
    var max:Long = Long.MinValue
    var avr:Double = 0
    var count: Long = 0

    def addValue(value:Long): Unit ={
      max = Math.max(max, value)
      min = Math.min(min, value)

      // prevent possible accuracy lost with big count: avr = (avr * count + value) / (count + 1.0)
      avr = avr * (count / (count + 1.0)) + value / (count + 1.0)
      count += 1
    }
  }

  class Experiment(val name:String) {
    var totalTime:Long = 0
    var totalProcessed: Long = 0
    var latency:Statistics = new Statistics
    var throughput: Statistics = new Statistics
    var values = new Array[String](0)
  }
}
