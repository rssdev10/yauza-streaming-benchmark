package helpers;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yauza.benchmark.common.helpers.DummyEvent;

import java.util.Map;

public class TestDataGenerator extends BaseRichSpout {
    private static final int eventsNum = 1000;// * 1000 * 10;
    private SpoutOutputCollector _collector;
    private int generatedEvents = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        if (generatedEvents < eventsNum) {
            String element = DummyEvent.generateJSON();
            generatedEvents++;

            if (element != null)
                _collector.emit(new Values(element));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("str"));
    }
}
