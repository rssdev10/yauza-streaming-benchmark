package yauza.benchmark.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    public static final String CONFIF_FILE_NAME = "conf/benchmark.conf";

    public static final String OUTPUT_DIR = "output";

    public static final String INPUT_TOPIC_PROP_NAME = "kafka.topic.input";
    public static final String INPUT_TOPIC_NAME = "yauza-input";

    public static final String OUTPUT_TOPIC_PROP_NAME_PREFIX = "kafka.output.out-";
    public static final String OUTPUT_TOPIC_NAME_PREFIX = "out-";

    public static final String PROP_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String PROP_ZOOKEEPER = "zookeeper.connect";

    public static final String PROP_PARTITIONS_NUMBER = "benchmark.runtime.partitions.number";
    public static final String PROP_WINDOW_DURATION = "benchmark.runtime.window.duration";

    public static final String PROP_DATA_DIRECTUPLOADER_THREADS = "benchmark.datagenerator.directuploader.threads";

    private Properties properties = null;

    public Config() {
    }

    public Config(String fileName) {
        properties = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(fileName);
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Config(Properties properties) {
        this.properties = properties;
    }

    public Properties getProperties() {
        return properties;
    }

    public Properties getKafkaProperties() {
        Properties kafkaProperties = new Properties();

        if (properties != null) {
            copyPropertyValue(kafkaProperties, PROP_BOOTSTRAP_SERVERS, "localhost:9092");
            copyPropertyValue(kafkaProperties, PROP_ZOOKEEPER, "localhost:2181");
        }

        kafkaProperties.put("group.id", properties.getProperty("kafka.group.id", "yauza"));

        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return kafkaProperties;
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    private void copyPropertyValue(Properties dest, String key, String defaultVal) {
        dest.put(key, properties.getProperty(key, defaultVal));
    }
}
