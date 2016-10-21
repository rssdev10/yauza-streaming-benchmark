package yauza.benchmark.spark;

import com.google.gson.Gson;
import kafka.serializer.StringDecoder;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import yauza.benchmark.common.Config;
import yauza.benchmark.common.Event;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SparkBenchmark {
    public static int partNum = 3;
    public static int windowDurationTime = 10;

    private static Gson gson = new Gson(); 

    public static <T> void main(String[] args) throws Exception {
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

        //CommandLineParser parser = new DefaultParser();
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(opts, args);

        String confFilename = cmd.getOptionValue("config");

        if (args.length == 0) {
            if (new File(Config.CONFIF_FILE_NAME).isFile()) {
                confFilename = Config.CONFIF_FILE_NAME;
            } else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("benchmark-spark", opts, true);
                System.exit(1);
            }
        }

        Properties props = new Properties();
        Arrays.asList(cmd.getOptions()).forEach(x -> {
            props.put(
                    x.hasLongOpt() ? x.getLongOpt() : x.getOpt(),
                    x.getValue()
            );
        });

        Config config;
        if (confFilename != null) {
            confFilename = FileSystems.getDefault().getPath(confFilename).normalize().toAbsolutePath().toString();
            config = new Config(confFilename);
        } else {
            config = new Config(props);
        }

        int numThreads = Integer.parseInt(config.getProperty("threads", "1"));
        String spark = config.getProperty("spark.master", "spark://localhost:7077");


        partNum = Integer.parseInt(config.getProperty(Config.PROP_PARTITIONS_NUMBER, "3"));
        windowDurationTime = Integer.parseInt(config.getProperty(Config.PROP_WINDOW_DURATION, "10"));

        Properties kafkaProps = config.getKafkaProperties();

        String topicList = kafkaProps.getProperty(Config.INPUT_TOPIC_PROP_NAME, Config.INPUT_TOPIC_NAME);
        String zooServers = kafkaProps.getProperty(Config.PROP_ZOOKEEPER, "localhost:2181");
        String bootstrapServers = kafkaProps.getProperty(Config.PROP_BOOTSTRAP_SERVERS, "localhost:9092");

        SparkConf sparkConf = new SparkConf()
                .setAppName("BenchmarkSpark")
                .set("spark.streaming.backpressure.enabled","true")
                // physical limit of processing speed
                .set("spark.streaming.receiver.maxRate", config.getProperty("spark.streaming.receiver.maxRate", "0"))
                .set("spark.streaming.kafka.maxRatePerPartition", config.getProperty("spark.streaming.kafka.maxRatePerPartition", "0"))
//                .setMaster("local");
                .setMaster(spark);

        // Create the context with 10 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Seconds.apply(windowDurationTime/2));

        //jssc.checkpoint("_checkpoint");

        // see: http://spark.apache.org/docs/latest/streaming-kafka-integration.html
//        Map<String, Integer> topicMap = new HashMap<String, Integer>();
//        String[] topics = topicList.split(",");
//        for (String topic: topics) {
//            topicMap.put(topic, numThreads);
//        }
//        JavaPairReceiverInputDStream<String, String> messages =
//                KafkaUtils.createStream(jssc, zooServers, "yauza", topicMap);

        Set<String> topicMap = new HashSet<>(Arrays.asList(topicList.split(",")));
        Map<String, String> kafkaParams = new HashMap<String, String>() {
            {
                put("metadata.broker.list", bootstrapServers);
                put("auto.offset.reset", "smallest");
                //put("auto.offset.reset", "largest");
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

            Broadcast<KafkaSink> kafkaSink = jssc.sparkContext().broadcast(new KafkaSink(kafkaProps));
            entry.getValue().foreachRDD(rdd -> {
                String topic = config.getProperty(
                        Config.OUTPUT_TOPIC_PROP_NAME_PREFIX + entry.getKey(),
                        Config.OUTPUT_TOPIC_NAME_PREFIX + entry.getKey());

                rdd.foreachPartition(
                        partitionOfRecords -> {
                            partitionOfRecords.forEachRemaining(x -> {
                                //System.out.print(x);
                                kafkaSink.value().send(topic,x);
                            });
                        });
            });
        }

        jssc.start();
        jssc.awaitTermination();
    }

    public static Map<String, JavaDStream<String>> buildTopology(JavaPairDStream<String, String> dataStream) {
        HashMap<String, JavaDStream<String>> result = new HashMap<String, JavaDStream<String>>();

        JavaDStream<Event> eventStream = dataStream.map(message -> {
            Event event = gson.fromJson(message._2(), Event.class);
            event.setInputTime();
            //System.out.print(json);
            return event;
        }).cache();

        result.put("uniq-users-number",
                UniqItems.transform(eventStream, (event) -> event.getUserId()));

        result.put("uniq-sessions-number",
                UniqItems.transform(eventStream, (event) -> event.getSessionId()));

        result.put("avr-price",
                AvrCounter.transform(
                        eventStream.filter(event -> {
                            String str = event.getReceiptId();
                            return str != null && str.length() > 0;
                        }),
                        (event) -> event.getPrice()));

        result.put("avr-session-duration",
                AvrDurationTimeCounter.transform(
                        eventStream.filter(event -> {
                            String str = event.getReceiptId();
                            return str == null || str.isEmpty();
                        }),
                        (event) -> event.getSessionId(),
                        (event) -> event.getUnixtimestamp()
                        )
                );
        return result;
    }
}
