package yauza.benchmark.flink;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

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
    public static DataStream<String> transform(DataStream<Event> eventStream, FieldAccessorString fieldAccessor) {
        KeyedStream<Event, Integer> userIdKeyed = eventStream
                .keyBy(event -> fieldAccessor.apply(event).getBytes()[0] % FlinkApp.partNum);

        WindowedStream<Event, Integer, TimeWindow> uniqUsersWin = userIdKeyed.timeWindow(Time.seconds(10));

        DataStream<UniqAggregator> uniqUsers = uniqUsersWin.trigger(ProcessingTimeTrigger.create())
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
                //System.out.println(value.toString());
                accumulator.value += value.uniqIds.size();

                accumulator.summarize(value);

                return accumulator;
            }

        }).map(x -> {
            Product product = new Product("UniqItems", Integer.toString(x.value));
            product.setStatistics(x);
            return product.toString();
        });
    }
}
