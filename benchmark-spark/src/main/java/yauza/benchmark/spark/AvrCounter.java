package yauza.benchmark.spark;

import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;
import yauza.benchmark.common.Event;
import yauza.benchmark.common.Product;
import yauza.benchmark.common.Statistics;
import yauza.benchmark.common.accessors.FieldAccessorLong;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static java.lang.Math.*;
import static yauza.benchmark.spark.SparkBenchmark.partNum;

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
    public static JavaDStream<String> transform(JavaDStream<Event> eventStream, FieldAccessorLong fieldAccessor) {

        JavaDStream<AverageAggregate> avrByPartitions = eventStream
                .mapToPair(x -> new Tuple2<Integer, Event>(fieldAccessor.apply(x).intValue() % partNum, x))
                .repartition(partNum)
                .mapPartitions(eventIterator -> {
                    AverageAggregate accumulator = new AverageAggregate();
                    eventIterator.forEachRemaining(new Consumer<Tuple2<Integer, Event>>() {
                        @Override
                        public void accept(Tuple2<Integer, Event> value) {
                            long count = accumulator.count;
                            long countNext = count + 1;
                            accumulator.average =
                                    accumulator.average * (count / (double) countNext) +
                                            fieldAccessor.apply(value._2()) / (double) countNext;

                            accumulator.registerEvent(value._2());
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
                });
    }
}
