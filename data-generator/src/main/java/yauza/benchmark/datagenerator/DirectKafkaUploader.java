package yauza.benchmark.datagenerator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import yauza.benchmark.common.Config;
import yauza.benchmark.common.Event;
import yauza.benchmark.common.helpers.DummyEvent;

import java.util.Properties;

public class DirectKafkaUploader implements Runnable {
    private static final long INTERVAL_MS = 1000;

    private final String topic;
    private final Config config;

    public DirectKafkaUploader(Config config, String topic) {
        this.config = config;
        DummyEvent.init(this.config);
        this.topic = topic;
    }

    @Override
    public void run() {
        KafkaProducer<String, String> producer = null;
        Properties props = config.getKafkaProperties();

        Long messagesNumber = Long.parseLong(
                config.getProperties().getProperty(
                        "benchmark.messages.number", Long.toString(HdfsWriter.eventsNum)));

        int limitPerSecond =
                Integer.parseInt(config.getProperties().getProperty(
                "benchmark.messages.per.second", Long.toString(HdfsWriter.eventsNum)));

        final int parallelThreads =
                Integer.parseInt(config.getProperties().getProperty(
                        Config.PROP_DATA_DIRECTUPLOADER_THREADS, Long.toString(2)));

        final int partitions =
                Integer.parseInt(config.getProperties().getProperty(Config.PROP_KAFKA_PARTITION, "3"));

        if (partitions > 1) {
            props.put("partitioner.class", "yauza.benchmark.datagenerator.KafkaPartitioner");
        }

        producer = new KafkaProducer<>(props);

        limitPerSecond = limitPerSecond / parallelThreads;

        long time = System.currentTimeMillis();
        for (long counter = 0, messages = 0; counter < messagesNumber; counter++) {
            messages++;
            Event event = DummyEvent.generate();
            String key = event.getUserId();
            String  value = DummyEvent.eventToString(event);
            producer.send(new ProducerRecord<>(topic, key, value));

            // ignore limit if limitPerSecond is zero
            if (limitPerSecond > 0 && messages >= limitPerSecond) {
                long curTime = System.currentTimeMillis();
                if (Math.abs(curTime - time) < INTERVAL_MS) {
                    try {
                        Thread.sleep(INTERVAL_MS - Math.abs(curTime - time));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                messages = 0;
                time = curTime;
            }
        }
    }
}
