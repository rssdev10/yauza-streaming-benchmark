package helpers;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import yauza.benchmark.common.helpers.DummyEvent;

public class TestDataGenerator {

    private static final int eventsNum = 1000;// * 1000 * 10;

    public static class UserEventsProducer extends Receiver<String> {

        private static final long serialVersionUID = -7548081699583411673L;
        private boolean isRunning;

        public UserEventsProducer() {
            super(StorageLevel.MEMORY_AND_DISK_2());
        }

        @Override
        public void onStart() {
            new Thread() {
                @Override
                public void run() {
                    generate();
                }
            }.start();
        }

        @Override
        public void onStop() {

        }

        public void generate(){
            for (int i = 0; i < eventsNum; i++) {
                String element = DummyEvent.generateJSON();
                // the source runs, isRunning flag should be checked frequently
                if (element != null)
                    store(element);
                //Thread.sleep(100);
            }
            stop("done test");
        }
    }

    static public Receiver<String> getDatastream() {
        return new UserEventsProducer();
    }
}
