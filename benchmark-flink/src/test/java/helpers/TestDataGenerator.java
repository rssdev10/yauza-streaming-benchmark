package helpers;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import yauza.benchmark.common.helpers.DummyEvent;

public class TestDataGenerator {

    private static final int eventsNum = 1000 * 1000 * 10;

    public static class UserEventsProducer implements SourceFunction<String> {

        private static final long serialVersionUID = -7548081699583411671L;
        private boolean isRunning;

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            // isRunning = true;
            // while (isRunning) {
            for (int i = 0; i < eventsNum; i++) {
                String element = DummyEvent.generateJSON();
                // the source runs, isRunning flag should be checked frequently
                if (element != null)
                    sourceContext.collect(element);
                //Thread.sleep(100);
            }
            Thread.sleep(30 * 1000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    static public SourceFunction<String> getDatastream() {
        return new UserEventsProducer();
    }
}
