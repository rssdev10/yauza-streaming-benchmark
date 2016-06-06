import org.junit.Test;

import helpers.TestDataGenerator;
import yauza.benchmark.flink.FlinkApp;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LocalFlinkTest {
    @Test public void testLocal() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.addSource(TestDataGenerator.getDatastream());
        Map<String, DataStream<String>> outputStreams = FlinkApp.buildTopology(dataStream);

        for (Entry<String, DataStream<String>> entry : outputStreams.entrySet()) {
            entry.getValue().print();
        }
        System.out.println(outputStreams.keySet().toString());
        assertTrue(outputStreams.size() > 0);

        env.execute();
    }
}
