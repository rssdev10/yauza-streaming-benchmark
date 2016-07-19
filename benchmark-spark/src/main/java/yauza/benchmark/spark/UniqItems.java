package yauza.benchmark.spark;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
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

        JavaDStream<UniqAggregator> uniques = eventStream
                .mapToPair(x -> new Tuple2<String, Event>(fieldAccessor.apply(x), x))
                .repartition(SparkBenchmark.partNum)
                .mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Event>>, UniqAggregator>() {
                    @Override
                    public Iterator<UniqAggregator> call(Iterator<Tuple2<String, Event>> eventIterator) throws Exception {
                        UniqAggregator accumulator = new UniqAggregator();
                        Set<String> uniqIds = new HashSet<String>();

                        eventIterator.forEachRemaining(new Consumer<Tuple2<String, Event>>() {
                            @Override
                            public void accept(Tuple2<String, Event> value) {
                                uniqIds.add(fieldAccessor.apply(value._2()));

                                accumulator.registerEvent(value._2());
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
                });

/*
        JavaPairDStream<String, Long> distinct = eventStream.map(event -> fieldAccessor.apply(event)).countByValue();
        return distinct.reduce(new Function2<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                return new Tuple2<String, Long>(null, v1._2 + v2._2);
            }
        }).map(x -> {
            Product product = new Product("UniqItems", x.toString());
            //product.setStatistics(x);
            return product.toString();
        });
*/
    }
}
