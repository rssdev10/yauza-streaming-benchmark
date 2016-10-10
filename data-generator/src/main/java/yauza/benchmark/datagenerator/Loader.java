package yauza.benchmark.datagenerator;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yauza.benchmark.common.Config;
import yauza.benchmark.common.Event;

import java.io.Serializable;
import java.util.Properties;

/**
 * Load file from local file system or from HDFS and upload it to the kafka's queue
 *
 */
public class Loader {
    private static final Logger LOG = LoggerFactory.getLogger(Loader.class);
    private static final long INTERVAL_MS = 1000;
    private static Gson gson = new Gson();

    public static void main(final String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 1) {
            printHelpMessage();
            System.exit(1);
        }

        String confFilename = parameterTool.get("config", Config.CONFIF_FILE_NAME);
        Config config;
        if (confFilename != null && !confFilename.isEmpty()) {
            config = new Config(confFilename);
        } else {
            config = new Config(parameterTool.getProperties());
        }

        String hdfsPath = config.getProperties().getProperty("benchmark.hdfs", "hdfs://localhost:9000");
        String dataFile = config.getProperties().getProperty("benchmark.datafile", "/yauza-benchmark/datafile.json");

        // read from command line !
        String mode = parameterTool.get("mode", "");

        if (mode.equals("generate_file")) {
            // generate data and write into HDFS
            new HdfsWriter().generate(hdfsPath, dataFile, config);
        } else if (mode.equals("load_to_kafka")) {
            // load data from HDFS and send to the Kafka's queue
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            String filePath = hdfsPath + dataFile;

            Properties kafkaProps = config.getKafkaProperties();

            final int limitPerSecond =
                    Integer.parseInt(config.getProperties().getProperty(
                            "benchmark.messages.per.second", Long.toString(HdfsWriter.eventsNum)));

            DataStream<String> dataStream = env.readFile(new DataFileInputFormat(filePath), filePath);
            if (limitPerSecond > 0) {
                dataStream = dataStream.map(new RichMapFunction<String, String>() {
                    private long time = System.currentTimeMillis();
                    private int messages = 0;

                    @Override
                    public String map(String value) throws Exception {
                        // limit per partition only
                        if (messages >= limitPerSecond) {
                            long curTime = System.currentTimeMillis();
                            if (Math.abs(curTime - time) < INTERVAL_MS) {
                                try {
                                    Thread.sleep(INTERVAL_MS - Math.abs(curTime - time));
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            messages = 0;
                            time = curTime;
                        }
                        messages++;
                        return value;
                    }
                });
            }

            final int partitions =
                    Integer.parseInt(config.getProperties().getProperty(Config.PROP_KAFKA_PARTITION, "3"));

            if (partitions > 1) {
                kafkaProps.put("partitioner.class", KafkaPartitioner.class.getCanonicalName());
                TypeInformation<Tuple2<String, String>> stringStringInfo =
                        TypeInfoParser.parse("org.apache.flink.api.java.tuple.Tuple2<String, String>");

                KeyedSerializationSchema<Tuple2<String, String>> schema =
                        new TypeInformationKeyValueSerializationSchema<>(String.class, String.class, env.getConfig());

                dataStream
                        .map(json -> {
                            Event event = gson.fromJson(json, Event.class);
                            return new Tuple2<String, String>(event.getUserId(), json);
                        }).returns(stringStringInfo)
                        .setParallelism(partitions)
                        .addSink(new FlinkKafkaProducer08<>(config.getProperty("topic", Config.INPUT_TOPIC_NAME),
                                schema,
                                //new KeyedSerializer(),
                                kafkaProps));
            } else {
//            dataStream.addSink(new FlinkKafkaProducer09<>(config.getProperty("topic", Config.INPUT_TOPIC_NAME),
//                    new SimpleStringSchema(),
//                    kafkaProps));

                dataStream.addSink(new FlinkKafkaProducer08<>(config.getProperty("topic", Config.INPUT_TOPIC_NAME),
                        new SimpleStringSchema(),
                        kafkaProps));
                //dataStream.print();
            }

            env.execute();
        } else if (mode.equals("inmemory")) {
            DirectKafkaUploader directUploader = new DirectKafkaUploader(config,
                    config.getProperty(Config.INPUT_TOPIC_PROP_NAME, Config.INPUT_TOPIC_NAME));

            final int parallelThreads =
                    Integer.parseInt(config.getProperties().getProperty(
                            Config.PROP_DATA_DIRECTUPLOADER_THREADS, Long.toString(2)));

            Thread threads[] = new Thread[parallelThreads];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(directUploader);
                threads[i].start();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].wait();
            }
        } else {
            printHelpMessage();
        }
        System.exit(1);
    }

    public static final class KeyedSerializer implements KeyedSerializationSchema<Tuple2<String, String>> {
        org.apache.kafka.common.serialization.StringSerializer serializer =
                new org.apache.kafka.common.serialization.StringSerializer();

        @Override
        public byte[] serializeKey(Tuple2<String, String> element) {
            return serializer.serialize("", element.f0);
        }

        @Override
        public byte[] serializeValue(Tuple2<String, String> element) {
            return serializer.serialize("", element.f1);
        }
    }

    private static void printHelpMessage() {
        System.out.println(
                "Missing parameters!\n" +
                        "Usage:\n" +
                        " data-generator --mode generate_file" +
                        " --benchmark.hdfs hdfs://localhost:9000" +
                        " --benchmark.datafile datafile.json" +

                        " data-generator --mode load_to_kafka" +
                        " --topic <Kafka topic>" +
                        " --bootstrap.servers <kafka brokers>" +
                        " --benchmark.hdfs hdfs://localhost:9000" +
                        " --benchmark.datafile datafile.json\n" +

                        " data-generator --mode inmemory --config conf/benchmark.conf"
        );
    }
}
