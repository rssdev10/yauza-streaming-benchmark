package yauza.benchmark.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.util.Properties;

/**
 * This class implements serializable kafka sink for Spark.
 *
 * Usage:
 * <PRE>
 *    Broadcast<KafkaSink> kafkaSink =
 *      jssc.sparkContext().broadcast(new KafkaSink(kafkaProps));
 *
 *    stream.foreachRDD(rdd -> {
 *      rdd.foreachPartition( artitionOfRecords -> {
 *        partitionOfRecords.forEachRemaining(x -> {
 *          kafkaSink.value().send(topic,x);
 *        });
 *      });
 *    });
 * </PRE>
 */
public class KafkaSink implements Serializable {
    private Properties config = null;
    private KafkaProducer<String, String> producer = null;

    public KafkaSink(Properties config) {
        this.config = config;
    }

    private KafkaSink() {
    }

    public void send(String topic, String value) {
        if (producer == null) {
            if (config != null) {
                init(config);
            }
        } else {
            producer.send(new ProducerRecord<>(topic, value));
        }
    }

    private void init(Properties config) {
        producer = new KafkaProducer<>(config);
        Runtime.getRuntime().addShutdownHook( new Thread () {
            @Override
            public void run() {
                producer.close();
            }
        });
    }
}
