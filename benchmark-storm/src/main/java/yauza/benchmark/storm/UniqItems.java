package yauza.benchmark.storm;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorString;

/**
 * This class implements aggregation by specified field and produces the number
 *  of unique items
 *
 */
public class UniqItems {

    static class UniqAggregator extends Statistics {
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
/*
        JavaDStream<UniqAggregator> uniques = eventStream
                .mapToPair(x -> new Tuple2<Integer, Event>(fieldAccessor.apply(x).getBytes()[0] % partNum, x))
                .groupByKey(partNum)
                .mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Iterable<Event>>>, UniqAggregator>() {
                    @Override
                    public Iterator<UniqAggregator> call(Iterator<Tuple2<Integer, Iterable<Event>>> tuple2Iterator) throws Exception {
                        UniqAggregator accumulator = new UniqAggregator();
                        Set<String> uniqIds = new HashSet<String>();

                        tuple2Iterator.forEachRemaining(new Consumer<Tuple2<Integer, Iterable<Event>>>() {
                            @Override
                            public void accept(Tuple2<Integer, Iterable<Event>> value) {
                                for (Event event: value._2()) {
                                    uniqIds.add(fieldAccessor.apply(event));

                                    accumulator.registerEvent(event);
                                }
                            }
                        });
                        accumulator.value = uniqIds.size();

                        List<UniqAggregator> list = Arrays.asList(accumulator);
                        return list.iterator();
                    }
                })
                .repartition(1);

        return uniques
                .reduce((x1, x2) -> {
                    x1.value += x2.value;

                    x1.summarize(x2);

                    return x1;
                })
                .map(aggregator -> {
                    Product product = new Product("UniqItems", aggregator.value.toString());
                    product.setStatistics(aggregator);
                    return product.toString();
                });*/

        return eventStream.each(new Fields("event"),
                new BaseFunction() {
                    @Override
                    public void execute(TridentTuple tuple, TridentCollector collector) {
                        collector.emit(new Values(tuple.get(0).toString()));
                    }
                },
                new Fields("result"));
    }
}
