package yauza.benchmark.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.google.gson.Gson;

import yauza.benchmark.common.Config;
import yauza.benchmark.common.Event;

public class FlinkApp {
    public static final int partNum = 3;
    public static final int emergencyTriggerTimeout = 3;

    private static Gson gson = new Gson(); 

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String confFilename = parameterTool.get("config");

        if (confFilename == null && parameterTool.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!\n"
                    + "Usage: benchmark-flink... --kafka.topic.input <topic> --bootstrap.servers <kafka brokers>"
                    + "\t or benchmark-flink --config <filename>");
            System.exit(1);
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Config config;
        if (confFilename != null) {
            config = new Config(confFilename);
        } else {
            config = new Config(parameterTool.getProperties());
        }

        Properties kafkaProps = config.getKafkaProperties();

        DataStream<String> dataStream = env
                .addSource(new FlinkKafkaConsumer08<String>(
                        config.getProperty(Config.INPUT_TOPIC_PROP_NAME, Config.INPUT_TOPIC_NAME),
                        new SimpleStringSchema(), kafkaProps));

        Map<String, DataStream<String>> outputStreams = buildTopology(dataStream);

        for (Entry<String, DataStream<String>> entry : outputStreams.entrySet()) {
            entry.getValue()
                    .addSink(new FlinkKafkaProducer08<String>(
                            config.getProperty(
                                    Config.OUTPUT_TOPIC_PROP_NAME_PREFIX + entry.getKey(),
                                    Config.OUTPUT_TOPIC_NAME_PREFIX + entry.getKey()),
                            new SimpleStringSchema(), kafkaProps));

            //entry.getValue().print();
        }

        env.execute();
    }

    public static Map<String, DataStream<String>> buildTopology(DataStream<String> dataStream) {
        HashMap<String, DataStream<String>> result = new HashMap<String, DataStream<String>>();
        DataStream<Event> eventStream = dataStream.map(json -> {
            Event event = gson.fromJson(json, Event.class);
            event.setInputTime();
//            System.out.print(json);
            return event;
        });

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
