package yauza.benchmark.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;
import yauza.benchmark.common.Event;
import yauza.benchmark.common.Product;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorLong;
import yauza.benchmark.common.accessors.FieldAccessorString;

import static yauza.benchmark.spark.SparkBenchmark.partNum;

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
        JavaDStream<AverageAggregate> avrByPartitions = eventStream
                .mapToPair(x -> new Tuple2<Integer, Event>(fieldAccessor.apply(x).getBytes()[0] % partNum, x))
                .groupByKey(partNum)
                .mapPartitions(eventIterator -> {
                    SessionAggregate accumulator = new SessionAggregate();
                    eventIterator.forEachRemaining(new Consumer<Tuple2<Integer, Iterable<Event>>>() {
                        @Override
                        public void accept(Tuple2<Integer, Iterable<Event>> value) {
                            for (Event event: value._2()) {
                                String key = fieldAccessor.apply(event);
                                TimeAggregate time = accumulator.sessions.get(key);
                                if (time == null) {
                                    time = new TimeAggregate();
                                }
                                time.lastTime = event.getUnixtimestamp();
                                if (time.firstTime == 0) {
                                    time.firstTime = time.lastTime;
                                }
                                accumulator.sessions.put(key, time);

                                accumulator.registerEvent(event);
                            }
                        }
                    });
                    List<SessionAggregate> list = Arrays.asList(accumulator);
                    return list.iterator();
                })
                .map(value -> {
                    long min = Long.MAX_VALUE;
                    long max = Long.MIN_VALUE;

                    double avr = 0;
                    double count = 0;
                    for (Entry<String, TimeAggregate> entry : value.sessions.entrySet()) {
                        long timeInterval = entry.getValue().lastTime - entry.getValue().firstTime;
                        // Check for completed sessions only.
                        // It means that at least two events must exist with different timestamps.
                        if (timeInterval > 0) {
                            avr = avr * (count / (count + 1)) + (timeInterval) / (count + 1);
                            count = count + 1;

                            if (min > timeInterval) {
                                min = timeInterval;
                            }

                            if (max < timeInterval) {
                                max = timeInterval;
                            }
                        }
                    }

                    AverageAggregate aggregate = new AverageAggregate(avr, (long) count, min, max);

                    aggregate.summarize(value);

                    return aggregate;
                })
                .repartition(1);

        return avrByPartitions
                .mapPartitions(eventIterator -> {
                    AverageAggregate accumulator = new AverageAggregate(0, 0);
                    eventIterator.forEachRemaining(new Consumer<AverageAggregate>() {
                        @Override
                        public void accept(AverageAggregate value) {
                            long countAcc = accumulator.count;
                            long countVal = value.count;
                            if (countAcc + value.count != 0) {
                                accumulator.average = accumulator.average * (countAcc * 1.0 / (countVal + countAcc))
                                        + value.average * (countVal * 1.0 / (countAcc + countVal));
                            }

                            if (accumulator.min > value.min ) {
                                accumulator.min = value.min;
                            }

                            if (accumulator.max < value.max) {
                                accumulator.max = value.max;
                            }

                            //System.out.println(value.toString());

                            accumulator.summarize(value);
                        }
                    });
                    List<AverageAggregate> list = Arrays.asList(accumulator);
                    return list.iterator();
                })
                .map(x -> {
                    Product product = new Product("AvrDurationTimeCounter", Long.toString((long) x.average));
                    product.setStatistics(x);
                    return product.toString();
                });
    }
}
