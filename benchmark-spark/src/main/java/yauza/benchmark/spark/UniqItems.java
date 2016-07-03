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
        public Set<String> uniqIds = new HashSet<String>();
    }

    static class ProductAggregator extends Statistics {
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

        JavaDStream<UniqAggregator> uniques = eventStream.mapPartitions(new FlatMapFunction<Iterator<Event>, UniqAggregator>() {
            @Override
            public Iterator<UniqAggregator> call(Iterator<Event> eventIterator) throws Exception {
                UniqAggregator accumulator = new UniqAggregator();

                eventIterator.forEachRemaining(new Consumer<Event>() {
                    @Override
                    public void accept(Event value) {
                        accumulator.uniqIds.add(fieldAccessor.apply(value));

                        accumulator.registerEvent(value);
                    }
                });

                List<UniqAggregator> list = Arrays.asList(accumulator);
                return list.iterator();
            }
        });

        return uniques
                .map(x -> new Tuple2<UniqAggregator, ProductAggregator>(x, null))
                .reduce((x1, x2) -> {
                    ProductAggregator accumulator = x1._2;
                    if (accumulator == null) {
                        accumulator = new ProductAggregator();
                    }
                    accumulator.value += x2._1.uniqIds.size();

                    accumulator.summarize(x2._1);

                    return new Tuple2<>(null, accumulator);
                })
                .map(x -> {
                    ProductAggregator aggregator = x._2;
                    Product product = new Product("UniqItems", aggregator.toString());
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