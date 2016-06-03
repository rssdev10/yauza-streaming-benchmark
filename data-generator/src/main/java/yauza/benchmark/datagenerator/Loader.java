package yauza.benchmark.datagenerator;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Load file from local file system or from HDFS and upload it to the kafka's queue
 *
 */
public class Loader {
    Logger LOG = LoggerFactory.getLogger(Loader.class);

    public static void main(final String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String hdfsPath = parameterTool.get("hdfs", "hdfs://localhost:9000");
        String dataFile = parameterTool.get("datafile", "/yauza-benchmark/datafile.json");

        if (parameterTool.getNumberOfParameters() < 1) {
            System.out.println(
                    "Missing parameters!\n" +
                            "Usage:\n" +
                    " data-generator --mode generate_file" +
                            " --hdfs hdfs://localhost:9000" +
                            " --datafile datafile.json" +

                    " data-generator --mode load_to_kafka" +
                            " --topic <Kafka topic>" +
                            " --bootstrap.servers <kafka brokers>" +
                            " --hdfs hdfs://localhost:9000" +
                            " --datafile datafile.json\n"
            );
            System.exit(1);
        }
        String mode = parameterTool.get("mode", "load_to_kafka");
        if (mode.equals("generate_file")) {
            new HdfsWriter().generate(hdfsPath, dataFile);
        } else {

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            String filePath = hdfsPath + dataFile;

            DataStream<String> dataStream = env.readFile(new DataFileInputFormat(filePath), filePath);
            dataStream.addSink(new FlinkKafkaProducer09<>(parameterTool.getRequired("topic"), new SimpleStringSchema(),
                    parameterTool.getProperties()));

            env.execute();
        }
    }
}
