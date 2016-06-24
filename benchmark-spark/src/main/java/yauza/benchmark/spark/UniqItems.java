package yauza.benchmark.spark;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
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
        JavaDStream<Event> uniqUsersWin = eventStream.repartition(SparkBenchmark.partNum).window(Duration.apply(10 * 1000));

/*        uniqUsers = uniqUsersWin.rdd
                .
                .fold(new UniqAggregator(), new FoldFunction<Event, UniqAggregator>() {
                    private static final long serialVersionUID = -6020094091742548382L;

                    @Override
                    public UniqAggregator fold(UniqAggregator accumulator, Event value) throws Exception {
                        accumulator.uniqIds.add(fieldAccessor.apply(value));

                        accumulator.registerEvent(value);

                        return accumulator;
                    }
                });

        AllWindowedStream<UniqAggregator, TimeWindow> combinedUniqNumStream =
                uniqUsers
                        .timeWindowAll(Time.seconds(FlinkApp.emergencyTriggerTimeout))
                        .trigger(PurgingTrigger.of(CountOrTimeTrigger.of(FlinkApp.partNum)));

        return combinedUniqNumStream.fold(new ProductAggregator(),
                new FoldFunction<UniqAggregator, ProductAggregator>() {
                    private static final long serialVersionUID = 7167358208807786523L;

                    @Override
                    public ProductAggregator fold(ProductAggregator accumulator, UniqAggregator value) throws Exception {
                        System.out.println(value.toString());
                        accumulator.value += value.uniqIds.size();

                        accumulator.summarize(value);

                        return accumulator;
                    }*/

        return eventStream.map(x -> {
            Product product = new Product("UniqItems", x.toString());
            return product.toString();
        });
    }
}
