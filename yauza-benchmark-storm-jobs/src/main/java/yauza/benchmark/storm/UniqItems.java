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
import yauza.benchmark.common.accessors.FieldAccessorString;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    public static Stream transform(Stream eventStream, FieldAccessorString fieldAccessor) {
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
                        new BaseAggregator<UniqAggregator>() {
                            @Override
                            public UniqAggregator init(Object batchId, TridentCollector collector) {
                                return new UniqAggregator();
                            }

                            @Override
                            public void aggregate(UniqAggregator accumulator, TridentTuple tuple, TridentCollector collector) {
                                Event event = (Event) tuple.get(0);
                                accumulator.uniqIds.add(fieldAccessor.apply(event));
                                accumulator.registerEvent(event);
                            }

                            @Override
                            public void complete(UniqAggregator val, TridentCollector collector) {
                                collector.emit(new Values(val));
                            }
                        },
                        new Fields("part_aggr")
                )
                .name("uid_win_part_aggr")
                .project(new Fields("part_aggr"))
//                .peek(new Consumer() {
//                    @Override
//                    public void accept(TridentTuple input) {
//                        System.out.print(input.get(0).toString());
//                    }
//                })
                .each(new Fields("part_aggr"),
                        // gathering all partial aggregates from all partitions
                        // TODO: check it again when Storm Trident supports partitioning in windows.
                        new BaseFunction() {
                            @Override
                            public void execute(TridentTuple tuple, TridentCollector collector) {
                                UniqAggregator part = (UniqAggregator) tuple.get(0);

                                ProductAggregator product = new ProductAggregator();
                                product.value = part.uniqIds.size();

                                product.summarize(part);

                                collector.emit(new Values(product));
                            }
                        },
                        new Fields("part_aggr_num")
                )
                .project(new Fields("part_aggr_num"))
                .aggregate(new Fields("part_aggr_num"),
                        new ReducerAggregator<ProductAggregator>() {
                            @Override
                            public ProductAggregator init() {
                                return new ProductAggregator();
                            }

                            @Override
                            public ProductAggregator reduce(ProductAggregator accumulator, TridentTuple tuple) {
                                ProductAggregator  part = (ProductAggregator ) tuple.get(0);
                                accumulator.value += part.value;

                                accumulator.summarize(part);

                                return accumulator;
                            }
                        },
                        new Fields("full_aggregate"))
                .name("uid_full_aggregate")
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
                                ProductAggregator x = (ProductAggregator) tuple.get(0);
                                Product product = new Product("UniqItems", Integer.toString(x.value));
                                product.setStatistics(x);
                                collector.emit(new Values(product.toString()));
                            }
                        },
                        new Fields("result"))
                .project(new Fields("result"));
    }
}
