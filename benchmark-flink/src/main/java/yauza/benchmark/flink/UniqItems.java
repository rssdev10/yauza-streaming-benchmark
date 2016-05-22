package yauza.benchmark.flink;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import yauza.benchmark.common.accessors.FieldAccessorString;
import yauza.benchmark.common.Event;

/**
 * This class implements aggregation by specified field and produces the number
 *  of unique items
 *
 */
public class UniqItems {
    private static final int partNum = 3;

    /**
     * Transform input stream and produce number of unique items
     *
     * @param eventStream input stream of Events
     * @param fieldAccessor field access function which returns string value
     * @return new chained stream
     */
    public static DataStream<String> transform(DataStream<Event> eventStream, FieldAccessorString fieldAccessor) {
        KeyedStream<Event, Integer> userIdKeyed = eventStream
                .keyBy(event -> fieldAccessor.apply(event).getBytes()[0] % partNum);

        WindowedStream<Event, Integer, TimeWindow> uniqUsersWin = userIdKeyed.timeWindow(Time.seconds(10));

        DataStream<Set<String>> uniqUsers = uniqUsersWin.trigger(ProcessingTimeTrigger.create())
                .fold(new HashSet<String>(), new FoldFunction<Event, Set<String>>() {
                    private static final long serialVersionUID = -6020094091742548382L;

                    @Override
                    public Set<String> fold(Set<String> accumulator, Event value) throws Exception {
                        accumulator.add(fieldAccessor.apply(value));
                        return accumulator;
                    }
                });

        AllWindowedStream<Set<String>, GlobalWindow> uniqUsers10sec = uniqUsers.countWindowAll(partNum);

        return uniqUsers10sec.fold(0, new FoldFunction<Set<String>, Integer>() {
            private static final long serialVersionUID = 7167358208807786523L;

            @Override
            public Integer fold(Integer accumulator, Set<String> value) throws Exception {
                System.out.println(value.toString());
                return accumulator + value.size();
            }

        }).map(x -> Integer.toString(x));
    }
}
