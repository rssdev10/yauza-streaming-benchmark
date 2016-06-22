package yauza.benchmark.spark;

import org.apache.spark.streaming.api.java.JavaDStream;
import yauza.benchmark.common.Event;
import yauza.benchmark.common.Product;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorLong;

/**
 * This class implements aggregation by specified field and calculation of
 * the average value
 */
public class AvrCounter {
    private static class AverageAggregate extends Statistics {
        public double average = 0.0;
        public long count = 0l;

        @Override
        public String toString() {
            return "AverageAggregate [average=" + average + ", count=" + count + "]";
        }
    }

    /**
     * Transform input stream and produce average value by specified field
     *
     * @param eventStream input stream of Events
     * @param fieldAccessor field access function which returns Long value
     * @return new chained stream
     */
    public static JavaDStream<String> transform(JavaDStream<Event> eventStream, FieldAccessorLong fieldAccessor) {
        return eventStream.map(x -> {
            Product product = new Product(
                    "AvrCounter",
                    x.toString());
//                    "Avr: " + Long.toString(Math.round(x.average)) + "; num: " + Long.toString(x.count));
            return product.toString();
        });
    }
}
