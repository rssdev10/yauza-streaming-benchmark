package yauza.benchmark.storm;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorLong;

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
/*
        JavaDStream<AverageAggregate> avrByPartitions = eventStream
                .repartition(partNum)
                .mapPartitions(eventIterator -> {
                    AverageAggregate accumulator = new AverageAggregate();
                    eventIterator.forEachRemaining(new Consumer<Event>() {
                        @Override
                        public void accept(Event value) {
                            long count = accumulator.count;
                            long countNext = count + 1;
                            accumulator.average =
                                    accumulator.average * (count / (double) countNext) +
                                            fieldAccessor.apply(value) / (double) countNext;

                            accumulator.registerEvent(value);
                        }
                    });

                    List<AverageAggregate> list = Arrays.asList(accumulator);
                    return list.iterator();
                })
                .repartition(1);

        return avrByPartitions
                .reduce((accumulator, value) -> {
                    System.out.println(value.toString());

                    long countAcc = accumulator.count;
                    long countVal = value.count;
                    if (countAcc + value.count != 0) {
                        accumulator.average = accumulator.average * (countAcc / (double)(countVal + countAcc))
                                + value.average * (countVal / (double)(countAcc + countVal));
                    }

                    accumulator.summarize(value);
                    return accumulator;
                })
                .map(x -> {
                    Product product = new Product(
                            "AvrCounter",
                            "Avr: " + Long.toString(round(x.average)) + "; num: " + Long.toString(x.count));
                    product.setStatistics(x);
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
