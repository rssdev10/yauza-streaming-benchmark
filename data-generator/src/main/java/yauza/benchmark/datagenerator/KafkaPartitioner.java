package yauza.benchmark.datagenerator;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class KafkaPartitioner implements Partitioner {

    public KafkaPartitioner  (VerifiableProperties props) {
    }

    @Override
    public int partition(Object key, int numPartitions) {
        return Math.abs(key.hashCode()) % numPartitions;
    }
}
