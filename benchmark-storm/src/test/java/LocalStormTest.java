import helpers.TestDataGenerator;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;
import org.junit.Test;
import yauza.benchmark.storm.StormBenchmark;

import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertTrue;


public class LocalStormTest {
    @Test public void testLocal() throws Exception {

        TridentTopology tridentTopology = new TridentTopology();

        Stream dataStream = tridentTopology
                .newStream("spout1", new TestDataGenerator()).parallelismHint(1);

        Map<String, Stream> outputStreams = StormBenchmark.buildTopology(dataStream);

        for (Entry<String, Stream> entry : outputStreams.entrySet()) {
            entry.getValue().each(new Fields("result"), new Debug());
        }
        System.out.println(outputStreams.keySet().toString());
        assertTrue(outputStreams.size() > 0);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("benchmark", StormBenchmark.getConsumerConfig(), tridentTopology.build());

        Thread.sleep(60 * 1000);

        cluster.killTopology("benchmark");
        cluster.shutdown();
    }
}
