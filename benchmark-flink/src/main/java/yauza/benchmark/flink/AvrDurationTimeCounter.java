package yauza.benchmark.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import yauza.benchmark.common.Event;
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

    private static class AverageAggregate {
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
    public static DataStream<String> transform(DataStream<Event> eventStream, FieldAccessorString fieldAccessor,
            FieldAccessorLong timestampAccessor) {
        KeyedStream<Event, Integer> keyedByIdHash = eventStream
                .keyBy(event -> fieldAccessor.apply(event).getBytes()[0] % partNum);

        WindowedStream<Event, Integer, TimeWindow> timedWindowStream =
                keyedByIdHash.timeWindow(Time.seconds(10));

        DataStream<Map<String, TimeAggregate>> timeIntervals =
                timedWindowStream.trigger(ProcessingTimeTrigger.create())
                .fold(new HashMap<String, TimeAggregate>(), new FoldFunction<Event, Map<String, TimeAggregate>>() {
                    private static final long serialVersionUID = -4469946090186220007L;

                    @Override
                    public Map<String, TimeAggregate> fold(Map<String, TimeAggregate> accumulator, Event value)
                            throws Exception {
                        String key = fieldAccessor.apply(value);
                        TimeAggregate time = accumulator.get(key);
                        if (time == null) {
                            time = new TimeAggregate();
                        }
                        time.lastTime = value.getUnixtimestamp();
                        if (time.firstTime == 0) {
                            time.firstTime = time.lastTime;
                        }
                        accumulator.put(key, time);
                        return accumulator;
                    }
                });

        SingleOutputStreamOperator<AverageAggregate> streamOfAverageIntervals = timeIntervals
                .map(new MapFunction<Map<String, TimeAggregate>, AverageAggregate>() {
                    private static final long serialVersionUID = -8915051786492935182L;

                    @Override
                    public AverageAggregate map(Map<String, TimeAggregate> value) throws Exception {
                        long min = Long.MAX_VALUE;
                        long max = Long.MIN_VALUE;

                        double avr = 0;
                        double count = 0;
                        for (Entry<String, TimeAggregate> entry : value.entrySet()) {
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

                        return new AverageAggregate(avr, (long) count, min, max);
                    }
                });

        AllWindowedStream<AverageAggregate, TimeWindow> winStreamOfAvrIntervals =
                streamOfAverageIntervals
                .timeWindowAll(Time.seconds(FlinkApp.emergencyTriggerTimeout))
                .trigger(PurgingTrigger.of(CountOrTimeTrigger.of(FlinkApp.partNum)));

        return winStreamOfAvrIntervals
                .fold(new AverageAggregate(0, 0), new FoldFunction<AverageAggregate, AverageAggregate>() {
                    private static final long serialVersionUID = -1802698606719661080L;

                    @Override
                    public AverageAggregate fold(AverageAggregate accumulator, AverageAggregate value)
                            throws Exception {
                        long countAcc = accumulator.count;
                        long countVal = value.count;
                        if (countAcc + value.count != 0) {
                            accumulator.average = accumulator.average * (countAcc * 1.0 / (countVal + countAcc))
                                    + value.average * (countVal * 1.0 / (countAcc + countVal));
                            accumulator.count = countAcc + countVal;
                        }

                        if (accumulator.min > value.min ) {
                            accumulator.min = value.min;
                        }

                        if (accumulator.max < value.max) {
                            accumulator.max = value.max;
                        }

                        System.out.println(value.toString());
                        return accumulator;
                    }
                }).map(x -> Long.toString((long) x.average));
    }
}
