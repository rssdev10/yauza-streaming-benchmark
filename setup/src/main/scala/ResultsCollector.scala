package yauza.benchmark.flink

import java.util.{Collections, Properties}

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import yauza.benchmark.common.Product

import scala.collection.mutable.ArrayBuffer

object ResultsCollector {
  private val gson: Gson = new Gson

  @throws[Exception]
  def main(args: Array[String]) {
    val gson = new Gson()

    Array(
      "uniq_users_number",
      "uniq_sessions_number",
      "avr_price",
      "avr_session_duration"
    )
      .par
      .map(str => "out_" + str) // add common suffix
      .map(queue => {
        val array = new Consumer(queue).run()
        array.foldLeft(new Experiment(queue)){(acc:Experiment, item:Product) => {
          acc.latency.addValue(item.getLatency)
          acc.throughput.addValue(item.getProcessedEvents / (item.getProcessingTime / 1000))
          acc.totalProcessed = acc.totalProcessed + item.getProcessedEvents
          acc.totalTime = acc.totalTime + item.getProcessingTime / 1000
          acc
        }}
      })
      .map(experiment => {
        gson.toJson(experiment)
      })
      .foreach(println)
  }

  class Consumer (val topic: String) {
    val props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "yauza");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    val consumer:KafkaConsumer[Integer, String] = new KafkaConsumer(props);
    import scala.collection.JavaConversions.asScalaIterator
    def run():ArrayBuffer[Product] = {
      var result = ArrayBuffer[Product]()
      consumer.subscribe(Collections.singletonList(topic));
      val records:ConsumerRecords[Integer, String] = consumer.poll(1000);
      println (s"Reading queue $topic. Found ${records.count()} messages.")

      for (record:ConsumerRecord[Integer, String] <- records.iterator()) {
        System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        val product = gson.fromJson(record.value(), classOf[Product])
        result += product
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
      count += 1
      max = Math.max(max, value)
      min = Math.min(min, value)
      avr = avr * (count / (count + 1.0)) + value / (count + 1.0)
    }
  }

  class Experiment(val name:String) {
    var totalTime:Long = _
    var totalProcessed: Long = _
    var latency:Statistics = _
    var throughput: Statistics = _
  }
}
