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

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class implements aggregation by specified field and calculation of
 * the average value
 */
public class AvrCounter {
    private static class AverageAggregate extends Statistics {
        public double average = 0.0;

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
    public static Stream transform(Stream eventStream, FieldAccessorLong fieldAccessor) {
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
                        new BaseAggregator<AverageAggregate>() {
                            @Override
                            public AverageAggregate init(Object batchId, TridentCollector collector) {
                                return new AverageAggregate();
                            }

                            @Override
                            public void aggregate(AverageAggregate val, TridentTuple tuple, TridentCollector collector) {
                                Event event = (Event) tuple.get(0);
                                long count = val.count;
                                long countNext = count + 1;
                                val.average =
                                        val.average * (count / (double) countNext) +
                                                fieldAccessor.apply(event) / (double) countNext;

                                val.registerEvent(event);
                            }

                            @Override
                            public void complete(AverageAggregate val, TridentCollector collector) {
                                collector.emit(new Values(val));
                            }
                        },
                        new Fields("part_aggr")
                )
                .name("avr_win_part_aggr")
                .project(new Fields("part_aggr"))
                .aggregate(new Fields("part_aggr"),
                        new ReducerAggregator<AverageAggregate>() {
                            // gathering all partial aggregates from all partitions
                            // TODO: check it again when Storm Trident supports partitioning in windows.

                            @Override
                            public AverageAggregate init() {
                                return new AverageAggregate();
                            }

                            @Override
                            public AverageAggregate reduce(AverageAggregate accumulator, TridentTuple tuple) {
                                AverageAggregate value = (AverageAggregate) tuple.get(0);
                                long countAcc = accumulator.count;
                                long countVal = value.count;
                                if (countAcc + value.count != 0) {
                                    accumulator.average = accumulator.average * (countAcc / (double)(countVal + countAcc))
                                            + value.average * (countVal / (double)(countAcc + countVal));
                                }
                                
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
                                Product product = new Product(
                                        "AvrCounter",
                                        "Avr: " + Long.toString(Math.round(x.average)) + "; num: " + Long.toString(x.count));
                                product.setStatistics(x);
                                collector.emit(new Values(product.toString()));
                            }
                        },
                        new Fields("result"))
                .project(new Fields("result"));
    }
}
