package helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.google.gson.Gson;

import yauza.benchmark.common.Event;

public class TestDataGenerator {
    private static Gson gson = new Gson();

    private static final int uNum = 30;
    private static final int percentageChangedSessions = 20;
    private static final int percentagePurchase = 10;

    private static int maxPrice = 10000; // in cents
    
    private static List<String> provider = new ArrayList<String>(){
        private static final long serialVersionUID = -2650978174049138472L;
    {
        add("play.google");
        add("app.apple");
        add("other.samsung");
        add("other.microsoft");
    }};

    private static int eventsNum = 1000;
    private static Random rand = new Random();

    private static final List<String> uId = new ArrayList<String>() {
        private static final long serialVersionUID = -2650978174049138472L;
        {
            for (int i = 0; i < uNum; i++) {
                add(UUID.randomUUID().toString());
            }
        }
    };

    public static class UserEventsProducer implements SourceFunction<String> {

        private static final long serialVersionUID = -7548081699583411671L;
        private boolean isRunning;

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            // isRunning = true;
            // while (isRunning) {
            for (int i = 0; i < eventsNum; i++) {
                String element = gson.toJson(dummyEvent());
                // the source runs, isRunning flag should be checked frequently
                if (element != null)
                    sourceContext.collect(element);
                // Thread.sleep(100);
            }
            Thread.sleep(15000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    static public SourceFunction<String> getDatastream() {
        return new UserEventsProducer();
    }

    static public Event dummyEvent() {
        Event event = new Event();
        event.setUserId(uId.get(rand.nextInt(uNum)));

        event.setSessionId(event.getUserId() +
                Integer.toString(rand.nextInt(100) < percentageChangedSessions ? 1 : 0));

        if (rand.nextInt(100) < percentagePurchase) {
            event.setProvider(provider.get(rand.nextInt(provider.size())));
            event.setReceiptId(UUID.randomUUID().toString());
            event.setPrice(new Long(rand.nextInt(maxPrice)));
        }

        return event;
    }
}
