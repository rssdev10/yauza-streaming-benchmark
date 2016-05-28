package yauza.benchmark.common.helpers;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.google.gson.Gson;

import yauza.benchmark.common.Event;

/**
  Generator of event sequence with predefined realistic fields.
*/
public class DummyEvent {
    private static Gson gson = new Gson();

    private static final int uNum = 30;
    private static final int percentageChangedSessions = 20;
    private static final int percentagePurchase = 10;

    private static final int maxPrice = 10000; // in cents

    private static long timestamp = 0;

    private static List<String> provider = new ArrayList<String>(){
        private static final long serialVersionUID = -2650978174049138472L;
    {
        add("play.google");
        add("app.apple");
        add("other.samsung");
        add("other.microsoft");
    }};

    private static final Random rand = new Random();

    private static final List<String> uId = new ArrayList<String>() {
        private static final long serialVersionUID = -2650978174049138472L;
        {
            for (int i = 0; i < uNum; i++) {
                add(UUID.randomUUID().toString());
            }
        }
    };

    static public String eventToString(Event event) {
        return gson.toJson(DummyEvent.generate());
    }

    static public Event generate() {
        Event event = new Event();
        event.setUserId(uId.get(rand.nextInt(uNum)));

        event.setSessionId(event.getUserId() +
                Integer.toString(rand.nextInt(100) < percentageChangedSessions ? 1 : 0));

        if (rand.nextInt(100) < percentagePurchase) {
            event.setProvider(provider.get(rand.nextInt(provider.size())));
            event.setReceiptId(UUID.randomUUID().toString());
            //
            event.setPrice(new Long(rand.nextInt(maxPrice)));
        }

        final SimpleDateFormat sdf = new SimpleDateFormat(Event.eventTimeFormat);
        event.setTimestamp(sdf.format(getNextTimestamp()));
        return event;
    }

    public static String generateJSON() {
        return eventToString(generate());
    }

    static private Object lock = new Object();
    public static long getNextTimestamp() {
        synchronized (lock) {
            if (timestamp == 0) {
                timestamp = new Date().getTime();
            } else {
                timestamp += (1 + rand.nextInt(100)) * 1000;
            }
        }
        return timestamp;
    }
}
