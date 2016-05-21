import org.junit.Test;

import yauza.benchmark.common.Event;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;

public class LibraryTest {
    @Test
    public void testEventTimestamp() {
        Event event = new Event();
        final SimpleDateFormat sdf = new SimpleDateFormat(Event.eventTimeFormat);
        Date date = new Date();

        event.setTimestamp(sdf.format(date));
        System.out.println(event.getTimestamp());
        assertEquals(date.getTime(), (long) event.getUnixtimestamp());
    }
}
