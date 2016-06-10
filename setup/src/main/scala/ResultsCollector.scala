package yauza.benchmark.flink


import java.util
import java.util.Properties

import com.google.gson.Gson
import kafka.consumer.ConsumerConfig
import kafka.javaapi.consumer.ConsumerConnector
import yauza.benchmark.common.Product

import scala.collection.mutable.ArrayBuffer

object ResultsCollector {
  private val gson: Gson = new Gson

  @throws[Exception]
  def main(args: Array[String]) {
/*    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
//    if (parameterTool.getNumberOfParameters < 2) {
//      System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>")
//      System.exit(1)
//    }

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = parameterTool.getProperties
//    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](parameterTool.get("topic", "yauza_input"), new SimpleStringSchema, properties))
//    val outputStreams: util.Map[String, DataStream[String]] = buildTopology(dataStream)
//    for (entry <- outputStreams.entrySet) {
//      entry.getValue.addSink(new FlinkKafkaProducer09[String](parameterTool.get("out_" + entry.getKey, "out_" + entry.getKey), new SimpleStringSchema, parameterTool.getProperties))
//    }

    val gson = new Gson()

    val streams: Map[String, DataStream[ArrayBuffer[Product]]] = Array(
      "uniq_users_number",
      "uniq_sessions_number",
      "avr_price",
      "avr_session_duration"
    )
      .map(queue => queue -> env.addSource(new FlinkKafkaConsumer09[String](queue, new SimpleStringSchema, properties))).toMap
      .mapValues(stream => {
        stream.map(new MapFunction[String, Product] {
          override def map(value: String): Product = {
            gson.fromJson(value, classOf[Product])
          }
        }).windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
          .trigger(CountTrigger.of(100))
          .fold(mutable.ArrayBuffer[Product](), new FoldFunction[Product, ArrayBuffer[Product]] {
            override def fold(accumulator: ArrayBuffer[Product], value: Product): ArrayBuffer[Product] = {
              accumulator += value
            }
          })
      })

    //streams.tail.fold(streams.head.)

    env.execute()*/

    val gson = new Gson()

    Array(
      "uniq_users_number",
      "uniq_sessions_number",
      "avr_price",
      "avr_session_duration"
    )
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
    val consumer: ConsumerConnector = kafka.consumer.Consumer
        .createJavaConsumerConnector(createConsumerConfig());

    def createConsumerConfig(): ConsumerConfig = {
      val props: Properties = new Properties()
      props.put("zookeeper.connect", "127.0.0.1:2181");
//      props.put("group.id", KafkaProperties.groupId);
      props.put("zookeeper.session.timeout.ms", "400");
      props.put("zookeeper.sync.time.ms", "200");
      props.put("auto.commit.interval.ms", "1000");

      return new ConsumerConfig(props);
    }

    def run():ArrayBuffer[Product] = {
      var result = ArrayBuffer[Product]()
      var topicCountMap = new util.HashMap[String, Integer]()
      topicCountMap.put(topic, new Integer(1));
      val consumerMap = consumer.createMessageStreams(topicCountMap);
      val stream = consumerMap.get(topic).get(0);

      val it = stream.iterator();
      while (it.hasNext()) {
        val message = new String(it.next().message());
        System.out.println(message);
        val product = gson.fromJson(message, classOf[Product])
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
