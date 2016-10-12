package yauza.benchmark.storm;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.TumblingDurationWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yauza.benchmark.common.Event;
import yauza.benchmark.common.Product;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorLong;
import yauza.benchmark.common.accessors.FieldAccessorString;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    public static Stream transform(Stream eventStream, FieldAccessorString fieldAccessor,
                                   FieldAccessorLong timestampAccessor) {
        return eventStream
                .each(new Fields("event"),
                        new BaseFunction() {
                            @Override
                            public void execute(TridentTuple tuple, TridentCollector collector) {
                                Event event = (Event) tuple.get(0);

                                collector.emit(new Values(fieldAccessor.apply(event)));
                            }
                        },
                        new Fields("key")
                )
                .partitionBy(new Fields("key"))
                .parallelismHint(StormBenchmark.partNum)
                .window(TumblingDurationWindow.of(new BaseWindowedBolt.Duration(StormBenchmark.windowDurationTime, TimeUnit.SECONDS)),
                        StormBenchmark.mapState,
                        new Fields("event"),
                        new BaseAggregator<SessionAggregate>() {
                            @Override
                            public SessionAggregate init(Object batchId, TridentCollector collector) {
                                return new SessionAggregate();
                            }

                            @Override
                            public void aggregate(SessionAggregate accumulator, TridentTuple tuple, TridentCollector collector) {
                                Event event = (Event) tuple.get(0);
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

                            @Override
                            public void complete(SessionAggregate val, TridentCollector collector) {
                                collector.emit(new Values(val));
                            }
                        },
                        new Fields("part_aggr")
                )
                .name("avr_time_win_part_aggr")
                .project(new Fields("part_aggr"))
                .each(new Fields("part_aggr"),
                        new BaseFunction() {

                            @Override
                            public void execute(TridentTuple tuple, TridentCollector collector) {
                                SessionAggregate value = (SessionAggregate) tuple.get(0);
                                long min = Long.MAX_VALUE;
                                long max = Long.MIN_VALUE;

                                double avr = 0;
                                double count = 0;
                                for (Map.Entry<String, TimeAggregate> entry : value.sessions.entrySet()) {
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

                                collector.emit(new Values(aggregate));
                            }
                        },
                        new Fields("part_aggr_with_time"))
                .project(new Fields("part_aggr_with_time"))
                .aggregate(new Fields("part_aggr_with_time"),
                        new ReducerAggregator<AverageAggregate>() {
                            // gathering all partial aggregates from all partitions
                            // TODO: check it again when Storm Trident supports partitioning in windows.

                            @Override
                            public AverageAggregate init() {
                                return new AverageAggregate(0, 0);
                            }

                            @Override
                            public AverageAggregate reduce(AverageAggregate accumulator, TridentTuple tuple) {
                                AverageAggregate value = (AverageAggregate) tuple.get(0);
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

                                return accumulator;
                            }
                        },
                        new Fields("full_aggregate"))
                .name("avr_full_aggregate")
                .filter(new Fields("full_aggregate"), new Filter() {
                    @Override
                    public boolean isKeep(TridentTuple tuple) {
                        Statistics aggregator = (Statistics) tuple.get(0);
                        return aggregator.count != 0;
                    }

                    @Override
                    public void prepare(Map conf, TridentOperationContext context) {

                    }

                    @Override
                    public void cleanup() {

                    }
                })
                .each(new Fields("full_aggregate"),
                        new BaseFunction() {
                            @Override
                            public void execute(TridentTuple tuple, TridentCollector collector) {
                                AverageAggregate x = (AverageAggregate) tuple.get(0);
                                Product product = new Product("AvrDurationTimeCounter", Long.toString((long) x.average));
                                product.setStatistics(x);

                                collector.emit(new Values(product.toString()));
                            }
                        },
                        new Fields("result"))
                .project(new Fields("result"));
    }
}
