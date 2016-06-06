package yauza.benchmark.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.google.gson.Gson;

import yauza.benchmark.common.Event;

public class FlinkApp {
    public static final int partNum = 3;
    public static final int emergencyTriggerTimeout = 3;

    private static Gson gson = new Gson(); 

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
            System.exit(1);
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = parameterTool.getProperties();
        DataStream<String> dataStream = env
                .addSource(new FlinkKafkaConsumer09<String>(parameterTool.get("topic", "yauza_input"),
                        new SimpleStringSchema(), properties));

        Map<String, DataStream<String>> outputStreams = buildTopology(dataStream);

        for (Entry<String, DataStream<String>> entry : outputStreams.entrySet()) {
            entry.getValue()
                    .addSink(new FlinkKafkaProducer09<String>(parameterTool.get("out_" + entry.getKey(), "out_" + entry.getKey()),
                            new SimpleStringSchema(), parameterTool.getProperties()));
        }

        env.execute();
    }

    public static Map<String, DataStream<String>> buildTopology(DataStream<String> dataStream) {
        HashMap<String, DataStream<String>> result = new HashMap<String, DataStream<String>>();
        DataStream<Event> eventStream = dataStream.map(json -> {
            return gson.fromJson(json, Event.class);
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
                            return str != null && str.length() > 0;
                        }),
                        (event) -> event.getSessionId(),
                        (event) -> event.getUnixtimestamp()
                        )
                );
        return result;
    }
}
