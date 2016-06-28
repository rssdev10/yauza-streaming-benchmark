package yauza.benchmark.spark;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import yauza.benchmark.common.Product;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorString;
import yauza.benchmark.common.Event;

/**
 * This class implements aggregation by specified field and produces the number
 *  of unique items
 *
 */
public class UniqItems {

    static class UniqAggregator extends Statistics {
        public Set<String> uniqIds = new HashSet<String>();
    }

    static class ProductAggregator extends Statistics{
        public Integer value = 0;
    }

    /**
     * Transform input stream and produce number of unique items
     *
     * @param eventStream input stream of Events
     * @param fieldAccessor field access function which returns string value
     * @return new chained stream
     */
    public static JavaDStream<String> transform(JavaDStream<Event> eventStream, FieldAccessorString fieldAccessor) {

        JavaPairDStream<String, Long> distinct = eventStream.map(event -> fieldAccessor.apply(event)).countByValue();
        return distinct
                .reduce((Function2<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>)
                    (v1, v2) -> new Tuple2<String, Long>(null, v1._2 + v2._2))
                .map(x -> {
            Product product = new Product("UniqItems", x.toString());
            //product.setStatistics(x);
            return product.toString();
        });
    }
}
