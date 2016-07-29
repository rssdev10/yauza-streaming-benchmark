package yauza.benchmark.datagenerator;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yauza.benchmark.common.Config;

import java.util.Properties;

/**
 * Load file from local file system or from HDFS and upload it to the kafka's queue
 *
 */
public class Loader {
    Logger LOG = LoggerFactory.getLogger(Loader.class);

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

            DataStream<String> dataStream = env.readFile(new DataFileInputFormat(filePath), filePath);
//            dataStream.addSink(new FlinkKafkaProducer09<>(parameterTool.getRequired("topic"), new SimpleStringSchema(),
//                    kafkaProps));
            dataStream.addSink(new FlinkKafkaProducer08<>(parameterTool.getRequired("topic"), new SimpleStringSchema(),
                    kafkaProps));

            //dataStream.print();

            env.execute();
        } else if (mode.equals("inmemory")) {
            new DirectKafkaUploader(config,
                    config.getProperty(Config.INPUT_TOPIC_PROP_NAME, Config.INPUT_TOPIC_NAME)).run();
        } else {
            printHelpMessage();
        }
        System.exit(1);
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
