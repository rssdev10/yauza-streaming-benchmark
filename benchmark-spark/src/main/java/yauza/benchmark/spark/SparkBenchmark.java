package yauza.benchmark.spark;

import com.google.gson.Gson;
import kafka.serializer.StringDecoder;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import scala.Tuple3;
import yauza.benchmark.common.Config;
import yauza.benchmark.common.Event;

import java.util.*;

public class SparkBenchmark {
    public static final int partNum = 3;
    public static final int emergencyTriggerTimeout = 3;

    private static Gson gson = new Gson(); 

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        OptionGroup group = new OptionGroup();
        group.addOption(new Option("topic", Config.INPUT_TOPIC_PROP_NAME, true, "Input topic name. <topic1, topic2>"));
        group.addOption(new Option("bootstrap", "bootstrap.servers", true, "Kafka brokers"));
        group.addOption(new Option("threads", "threads", true, "Number of threads per each topic"));
        group.addOption(new Option("spark", "spark.master", true, "URL to Spark master host. <spark://master_ip:7077>"));
        opts.addOptionGroup(group);

        group = new OptionGroup();
        group.addOption(new Option("config", "config", true, "Configuration file name."));
        opts.addOptionGroup(group);

        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("benchmark-spark", opts, true);
            System.exit(1);
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);

        String confFilename = cmd.getOptionValue("config");

        Properties props = new Properties();
        Arrays.asList(cmd.getOptions()).forEach(x -> {
            props.put(
                    x.hasLongOpt() ? x.getLongOpt() : x.getOpt(),
                    x.getValue()
            );
        });

        Config config;
        if (confFilename != null) {
            config = new Config(confFilename);
        } else {
            config = new Config(props);
        }

        int numThreads = Integer.parseInt(config.getProperty("threads", "1"));
        String spark = config.getProperty("spark.master", "spark://localhost:7077");

        Properties kafkaProps = config.getKafkaProperties();

        String topicList = kafkaProps.getProperty(Config.INPUT_TOPIC_PROP_NAME, Config.INPUT_TOPIC_NAME);
        String zooServers = kafkaProps.getProperty(Config.PROP_ZOOKEEPER, "localhost:2181");
        String bootstrapServers = kafkaProps.getProperty(Config.PROP_BOOTSTRAP_SERVERS, "localhost:9092");

        SparkConf sparkConf = new SparkConf()
                .setAppName("BenchmarkSpark")
                .set("spark.streaming.backpressure.enabled","true")
                // uncomment it to set phisical limit of processing
                // .set("spark.streaming.receiver.maxRate", "10000")
                // .set("spark.streaming.kafka.maxRatePerPartition", "10000")
                .setMaster("local");
//                .setMaster(spark);

        // Create the context with 10 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Milliseconds.apply(5000));

        jssc.checkpoint("_checkpoint");

        // see: http://spark.apache.org/docs/latest/streaming-kafka-integration.html
/*
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = topicList.split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zooServers, "yauza", topicMap);
*/

        Set<String> topicMap = new HashSet<>(Arrays.asList(topicList.split(",")));
        Map<String, String> kafkaParams = new HashMap<String, String>() {
            {
                put("metadata.broker.list", bootstrapServers);
                put("auto.offset.reset", "smallest");
            }
        };

        JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicMap);

        Map<String, JavaDStream<String>> outputStreams = buildTopology(messages);

        for (Map.Entry<String, JavaDStream<String>> entry : outputStreams.entrySet()) {
            entry.getValue().print();

            entry.getValue().foreachRDD(rdd -> {
                rdd.foreachPartition(
                        partitionOfRecords -> {
                            partitionOfRecords.forEachRemaining(x -> {
                                System.out.print(x);
//                                // not optimal but does not require serializing
//                                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
//
//                                ProducerRecord<String, String> message = new ProducerRecord<String, String>(
//                                        config.getProperty(
//                                                Config.OUTPUT_TOPIC_PROP_NAME_PREFIX + entry.getKey(),
//                                                Config.OUTPUT_TOPIC_NAME_PREFIX + entry.getKey()),
//                                        null, x);
//                                producer.send(message);
                            });
                        });
            });
        }

        jssc.start();
        jssc.awaitTermination();
    }

    public static Map<String, JavaDStream<String>> buildTopology(JavaPairInputDStream<String, String> dataStream) {
        HashMap<String, JavaDStream<String>> result = new HashMap<String, JavaDStream<String>>();

        JavaDStream<Event> eventStream = dataStream.map(message -> {
            Event event = gson.fromJson(message._2(), Event.class);
            event.setInputTime();
            //System.out.print(json);
            return event;
        }).window(Seconds.apply(10));

        result.put("uniq_users_number",
                UniqItems.transform(eventStream, (event) -> event.getUserId()));

/*        result.put("uniq_sessions_number",
                UniqItems.transform(eventStream, (event) -> event.getSessionId()));

        result.put("avr_price",
                AvrCounter.transform(
                        eventStream.filter(event -> {
                            String str = event.getReceiptId();
                            return str != null && str.length() > 0;
                        }),
                        (event) -> event.getPrice()));

        result.put("avr_session_duration",
                AvrDurationTimeCounter.transform(
                        eventStream.filter(event -> {
                            String str = event.getReceiptId();
                            return str == null || str.isEmpty();
                        }),
                        (event) -> event.getSessionId(),
                        (event) -> event.getUnixtimestamp()
                        )
                );*/
        return result;
    }
}
