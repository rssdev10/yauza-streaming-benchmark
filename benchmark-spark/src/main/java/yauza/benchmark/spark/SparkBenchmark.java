package yauza.benchmark.spark;

import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import yauza.benchmark.common.Event;

import java.util.HashMap;
import java.util.Map;

public class SparkBenchmark {
    public static final int partNum = 3;
    public static final int emergencyTriggerTimeout = 3;

    public static final String outPrefix = "out-";

    private static Gson gson = new Gson(); 

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println(
                    "Missing parameters!\n"
                    + "Usage: benchmark-spark --topic <topic1, topic2> --bootstrap.servers <kafka brokers>"
                    + " --spark <spark://master:7077> --threads <threads per topic>");
            System.exit(1);
        }

        Options opts = new Options();
        opts.addOption("topic", true, "Input topic name.");
        opts.addOption("bootstrap.servers", true, "Kafka brokers");
        opts.addOption("threads", true, "Number of threads per each topic");
        opts.addOption("spark", true, "URL to Spark master host");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);

        String topicList = cmd.getOptionValue("topic", "yauza-input");
        String zooServers = cmd.getOptionValue("bootstrap.servers", "localhost:9092");
        Integer numThreads = Integer.parseInt(cmd.getOptionValue("threads", "1"));
        String spark = cmd.getOptionValue("spark", "spark://localhost:7077");

        SparkConf sparkConf = new SparkConf()
                .setAppName("BenchmarkSpark")
                .setMaster(spark);

        // Create the context with 10 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10 * 1000));

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = topicList.split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zooServers, "yauza", topicMap);

        JavaDStream<String> jsons = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        Map<String, JavaDStream<String>> outputStreams = buildTopology(jsons);

        for (Map.Entry<String, JavaDStream<String>> entry : outputStreams.entrySet()) {
            entry.getValue().print();
        }

        jssc.start();
        jssc.awaitTermination();
    }

    public static Map<String, JavaDStream<String>> buildTopology(JavaDStream<String> dataStream) {
        HashMap<String, JavaDStream<String>> result = new HashMap<String, JavaDStream<String>>();

        JavaDStream<Event> eventStream = dataStream.map(json -> {
            Event event = gson.fromJson(json, Event.class);
            event.setInputTime();
            //System.out.print(json);
            return event;
        });

        result.put("uniq_users_number",
                UniqItems.transform(eventStream, (event) -> event.getUserId()));

        result.put("uniq_sessions_number",
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
                );
        return result;
    }
}
