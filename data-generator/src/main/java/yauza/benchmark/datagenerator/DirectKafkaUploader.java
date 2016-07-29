package yauza.benchmark.datagenerator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import yauza.benchmark.common.Config;
import yauza.benchmark.common.helpers.DummyEvent;

import java.util.Properties;

public class DirectKafkaUploader implements Runnable {
    private static long INTERVAL_MS = 1000;

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
        producer = new KafkaProducer<>(props);

        final int limitPerSecond =
                Integer.parseInt(config.getProperties().getProperty(
                "benchmark.messages.per.second", Long.toString(HdfsWriter.eventsNum)));

        long time = System.currentTimeMillis();
        int messages = 0;
        while (true) {
            messages++;
            String value = DummyEvent.generateJSON();
            producer.send(new ProducerRecord<>(topic, value));

            if (messages >= limitPerSecond) {
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
