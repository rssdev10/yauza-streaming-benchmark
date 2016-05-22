package yauza.benchmark.flink;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import yauza.benchmark.common.Event;
import yauza.benchmark.common.accessors.FieldAccessorLong;

/**
 * This class implements aggregation by specified field and calculation of
 * the average value
 *
 */
public class AvrCounter {
    private static final int partNum = 3;

    private static class AverageAggregate {
        public double average = 0.0;
        public long count = 0l;

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
        KeyedStream<Event, Integer> userIdKeyed = eventStream
                .keyBy(event -> fieldAccessor.apply(event).intValue() % partNum);

        WindowedStream<Event, Integer, TimeWindow> uniqUsersWin = userIdKeyed.timeWindow(Time.seconds(10));

        DataStream<AverageAggregate> uniqUsers = uniqUsersWin.trigger(ProcessingTimeTrigger.create())
                .fold(new AverageAggregate(), new FoldFunction<Event, AverageAggregate>() {
                    private static final long serialVersionUID = -5253000612821767427L;

                    @Override
                    public AverageAggregate fold(AverageAggregate accumulator, Event event) throws Exception {
                        long count = accumulator.count;
                        accumulator.average =
                                accumulator.average * (count * 1.0 / (count + 1)) + 
                                fieldAccessor.apply(event) / (count + 1);
                        accumulator.count = count + 1;
                        return accumulator;
                    }
                });

        AllWindowedStream<AverageAggregate, GlobalWindow> uniqUsers10sec = uniqUsers.countWindowAll(partNum);

        return uniqUsers10sec.fold(new AverageAggregate(), new FoldFunction<AverageAggregate, AverageAggregate>() {
            private static final long serialVersionUID = -3856225899958993160L;

            @Override
            public AverageAggregate fold(AverageAggregate accumulator, AverageAggregate value) throws Exception {
                System.out.println(value.toString());

                long countAcc = accumulator.count;
                long countVal = value.count;
                if (countAcc + value.count != 0) {
                    accumulator.average = accumulator.average * (countAcc * 1.0 / (countVal + countAcc))
                            + value.average * (countVal * 1.0 / (countAcc + countVal));
                    accumulator.count = countAcc + countVal;
                }
                return accumulator;
            }
        }).map(x -> "Avr: " + Long.toString((long) x.average) + "; num: " + Long.toString(x.count));
    }
}
