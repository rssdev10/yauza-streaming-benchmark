import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import helpers.TestDataGenerator;
import scala.Tuple2;
import yauza.benchmark.spark.SparkBenchmark;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Map.Entry;


public class LocalSparkTest {
    @Test public void testLocal() throws Exception {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BenchmarkSparkTest")
                .setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10 * 1000));


        JavaPairDStream<String, String> dataStream =
                jssc.receiverStream(TestDataGenerator.getDatastream())
                .mapToPair(x -> new Tuple2<String, String>("", x));
        Map<String, JavaDStream<String>> outputStreams = SparkBenchmark.buildTopology(dataStream);

        for (Entry<String, JavaDStream<String>> entry : outputStreams.entrySet()) {
            entry.getValue().print();
        }
        System.out.println(outputStreams.keySet().toString());
        assertTrue(outputStreams.size() > 0);

        jssc.start();
        // jssc.awaitTermination(30 * 1000); // <=1.6
        jssc.awaitTerminationOrTimeout(30 * 1000); // >=2.0
    }
}
