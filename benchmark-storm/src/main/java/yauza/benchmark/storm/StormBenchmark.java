package yauza.benchmark.storm;

import com.google.gson.Gson;
import org.apache.commons.cli.*;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yauza.benchmark.common.Config;
import yauza.benchmark.common.Event;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StormBenchmark {
    public static int partNum = 3;
    public static int windowDurationTime = 10;

    public static final WindowsStoreFactory mapState = new InMemoryWindowsStoreFactory();

    private static final Gson gson = new Gson();

    public static org.apache.storm.Config getConsumerConfig() {
        org.apache.storm.Config conf = new org.apache.storm.Config();
        conf.setMaxSpoutPending(20);
        //  conf.setDebug(true);
        return conf;
    }

    private static TransactionalTridentKafkaSpout createKafkaSpout(ZkHosts hosts, String topic) {
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(hosts, topic);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Consume new data from the topic
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TransactionalTridentKafkaSpout result = new TransactionalTridentKafkaSpout(spoutConfig);
        Fields a = result.getOutputFields();
        return result;
    }

    public static class DeserializeEvent extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            Event event = gson.fromJson(sentence, Event.class);
            event.setInputTime();

            collector.emit(new Values(event));
        }
    }

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        OptionGroup group = new OptionGroup();
        group.addOption(new Option("topic", Config.INPUT_TOPIC_PROP_NAME, true, "Input topic name. <topic1, topic2>"));
        group.addOption(new Option("bootstrap", "bootstrap.servers", true, "Kafka brokers"));
        group.addOption(new Option("threads", "threads", true, "Number of threads per each topic"));
        opts.addOptionGroup(group);

        group = new OptionGroup();
        group.addOption(new Option("config", "config", true, "Configuration file name."));
        opts.addOptionGroup(group);

        //CommandLineParser parser = new DefaultParser();
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(opts, args);

        String confFilename = cmd.getOptionValue("config");

        if (args.length == 0) {
            if (new File(Config.CONFIF_FILE_NAME).isFile()) {
                confFilename = Config.CONFIF_FILE_NAME;
            } else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("benchmark-storm", opts, true);
                System.exit(1);
            }
        }

        Properties props = new Properties();
        Arrays.asList(cmd.getOptions()).forEach(x -> {
            props.put(
                    x.hasLongOpt() ? x.getLongOpt() : x.getOpt(),
                    x.getValue()
            );
        });

        Config config;
        if (confFilename != null) {
            config = new Config(confFilename);
        } else {
            config = new Config(props);
        }

        partNum = Integer.parseInt(config.getProperty(Config.PROP_PARTITIONS_NUMBER, "3"));
        windowDurationTime = Integer.parseInt(config.getProperty(Config.PROP_WINDOW_DURATION, "10"));

        Properties kafkaProps = config.getKafkaProperties();

        String topicList = kafkaProps.getProperty(Config.INPUT_TOPIC_PROP_NAME, Config.INPUT_TOPIC_NAME);
        String zooServers = kafkaProps.getProperty(Config.PROP_ZOOKEEPER, "localhost:2181");
        String bootstrapServers = kafkaProps.getProperty(Config.PROP_BOOTSTRAP_SERVERS, "localhost:9092");

        int workers = Integer.parseInt(config.getProperty("storm.workers","1"));
        int ackers = Integer.parseInt(config.getProperty("storm.ackers", "2"));
        int cores = Integer.parseInt(config.getProperty("process.cores", "3"));
        int parallel = Math.max(1, cores/7);

        ZkHosts hosts = new ZkHosts(zooServers);
//        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicList, "/" + topicList, UUID.randomUUID().toString());
//        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TridentTopology tridentTopology = new TridentTopology();

        Stream dataStream = tridentTopology
                .newStream("spout1", createKafkaSpout(hosts,topicList)).parallelismHint(1);
//                .each(new Fields("str"), new Debug());

        Properties outProps = config.getKafkaProperties();

        Map<String, Stream> outputStreams = buildTopology(dataStream);

        for (Map.Entry<String, Stream> entry : outputStreams.entrySet()) {
            Stream stream = entry.getValue();

            TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                    .withProducerProperties(outProps)
                    .withKafkaTopicSelector(new DefaultTopicSelector(config.getProperty(
                            Config.OUTPUT_TOPIC_PROP_NAME_PREFIX + entry.getKey(),
                            Config.OUTPUT_TOPIC_NAME_PREFIX + entry.getKey())))
                    .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper(null, "result"));

            stream
                    .each(new Fields("result"), new Debug())
                    .partitionPersist(stateFactory, new Fields("result"), new TridentKafkaUpdater());
        }

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("benchmark", getConsumerConfig(), tridentTopology.build());

        Thread.sleep(Integer.parseInt(config.getProperty(Config.PROP_TEST_DURATION, "60")) * 1000);

        cluster.killTopology("benchmark");
        cluster.shutdown();

    }

    public static HashMap<String, Stream> buildTopology(Stream stream) {

        HashMap<String, Stream> result = new HashMap<>();

        Stream eventStream = stream.each(new Fields("str"), new DeserializeEvent(), new Fields("event"))
                .project(new Fields("event"));

        result.put("uniq-users-number",
                UniqItems.transform(eventStream, (event) -> event.getUserId()));

        result.put("uniq-sessions-number",
                UniqItems.transform(eventStream, (event) -> event.getSessionId()));

        result.put("avr-price",
                AvrCounter.transform(
                        eventStream.filter(
                                new Fields("event"),
                                new Filter() {
                                    @Override
                                    public boolean isKeep(TridentTuple tuple) {
                                        Event event = (Event) tuple.get(0);
                                        String str = event.getReceiptId();
                                        return str != null && str.length() > 0;
                                    }

                                    @Override
                                    public void prepare(Map conf, TridentOperationContext context) {

                                    }

                                    @Override
                                    public void cleanup() {

                                    }
                                }),
                        (event) -> event.getPrice()));

        result.put("avr-session-duration",
                AvrDurationTimeCounter.transform(
                        eventStream.filter(
                                new Fields("event"),
                                new Filter() {
                                    @Override
                                    public boolean isKeep(TridentTuple tuple) {
                                        Event event = (Event) tuple.get(0);
                                        String str = event.getReceiptId();
                                        return str == null || str.isEmpty();

                                    }

                                    @Override
                                    public void prepare(Map conf, TridentOperationContext context) {

                                    }

                                    @Override
                                    public void cleanup() {

                                    }
                                }
                        ),
                        (event) -> event.getSessionId(),
                        (event) -> event.getUnixtimestamp()
                )
        );
        return result;
    }
}
