package yauza.benchmark.flink;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import yauza.benchmark.common.Event;
import yauza.benchmark.common.Product;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorLong;

/**
 * This class implements aggregation by specified field and calculation of
 * the average value
 *
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
    public static DataStream<String> transform(DataStream<Event> eventStream, FieldAccessorLong fieldAccessor) {
        KeyedStream<Event, Integer> streamOfNumerics = eventStream
                .keyBy(event -> event.partition(FlinkApp.partNum));

        WindowedStream<Event, Integer, TimeWindow> windowedStream =
                streamOfNumerics.timeWindow(Time.seconds(FlinkApp.windowDurationTime));

        DataStream<AverageAggregate> streamOfAverage =
                windowedStream.trigger(ProcessingTimeTrigger.create())
                .fold(new AverageAggregate(), new FoldFunction<Event, AverageAggregate>() {
                    private static final long serialVersionUID = -5253000612821767427L;

                    @Override
                    public AverageAggregate fold(AverageAggregate accumulator, Event event) throws Exception {
                        long count = accumulator.count;
                        long countNext = count + 1;
                        accumulator.average =
                                accumulator.average * (count / (double)countNext) +
                                fieldAccessor.apply(event) / (double)countNext;

                        accumulator.registerEvent(event);

                        return accumulator;
                    }
                });

        AllWindowedStream<AverageAggregate, TimeWindow> combinedStreamOfAverage =
                streamOfAverage.timeWindowAll(Time.seconds(FlinkApp.emergencyTriggerTimeout))
                        .trigger(PurgingTrigger.of(CountOrTimeTrigger.of(FlinkApp.partNum)));

        return combinedStreamOfAverage.fold(new AverageAggregate(),
                new FoldFunction<AverageAggregate, AverageAggregate>() {
            private static final long serialVersionUID = -3856225899958993160L;

            @Override
            public AverageAggregate fold(AverageAggregate accumulator, AverageAggregate value) throws Exception {
                //System.out.println(value.toString());

                long countAcc = accumulator.count;
                long countVal = value.count;
                if (countAcc + value.count != 0) {
                    accumulator.average = accumulator.average * (countAcc / (double)(countVal + countAcc))
                            + value.average * (countVal / (double)(countAcc + countVal));
                }

                accumulator.summarize(value);

                return accumulator;
            }
        }).map(x -> {
            Product product = new Product(
                    "AvrCounter",
                    "Avr: " + Long.toString(Math.round(x.average)) + "; num: " + Long.toString(x.count));
            product.setStatistics(x);
            return product.toString();
        });
    }
}
