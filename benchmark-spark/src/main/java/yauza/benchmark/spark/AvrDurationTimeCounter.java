package yauza.benchmark.spark;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.streaming.api.java.JavaDStream;
import yauza.benchmark.common.Event;
import yauza.benchmark.common.Product;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorLong;
import yauza.benchmark.common.accessors.FieldAccessorString;

/**
 * This class implements calculation of average duration of sessions by specified
 * id and timestamp fields
 *
 */
public class AvrDurationTimeCounter {
    private static final int partNum = 3;

    private static class TimeAggregate {
        public long firstTime;
        public long lastTime;
    }

    private static class SessionAggregate extends Statistics {
        Map<String, TimeAggregate> sessions = new HashMap<String, TimeAggregate>();
    }

    private static class AverageAggregate extends Statistics{
        public double average = 0.0;
        public long min = Long.MAX_VALUE;
        public long max = Long.MIN_VALUE;
        public long count = 0l;

        public AverageAggregate(double average, long count) {
            this.average = average;
            this.count = count;
        }

        public AverageAggregate(double average, long count, long min, long max) {
            this.average = average;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        @Override
        public String toString() {
            return "AverageAggregate [average=" + average + ", min=" + min + ", max=" + max + ", count=" + count + "]";
        }

    }

    /**
     * Transform input stream and produce average duration of events
     * by id from specified field
     *
     * @param eventStream input stream of Events
     * @param fieldAccessor field access function which returns Long value
     * @param timestampAccessor field access function which returns timestamp of the event
     * @return new chained stream
     */
    public static JavaDStream<String> transform(JavaDStream<Event> eventStream, FieldAccessorString fieldAccessor,
                                                FieldAccessorLong timestampAccessor) {
        return eventStream.map(x -> {
                    Product product = new Product("AvrDurationTimeCounter", x.toString());
                    return product.toString();
                });
    }
}
