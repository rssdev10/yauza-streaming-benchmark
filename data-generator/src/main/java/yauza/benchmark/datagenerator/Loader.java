package yauza.benchmark.datagenerator;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load file from local file system or from HDFS and upload it to the kafka's queue
 *
 */
public class Loader {
    Logger LOG = LoggerFactory.getLogger(Loader.class);

    public static void main(final String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> --bootstrap.servers <kafka brokers> --datafile datafile.json");
            System.exit(1);
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = parameterTool.get("datafile", "datafile.json");

        DataStream<String> dataStream = env.readFile(new DataFileInputFormat(filePath), filePath);
        dataStream.addSink(new FlinkKafkaProducer09<>(parameterTool.getRequired("topic"), new SimpleStringSchema(),
                parameterTool.getProperties()));

        env.execute();
    }
}
