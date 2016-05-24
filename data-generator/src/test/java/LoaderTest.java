import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.gson.Gson;

import yauza.benchmark.common.Event;
import yauza.benchmark.common.helpers.DummyEvent;
import yauza.benchmark.datagenerator.DataFileInputFormat;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LoaderTest {
    private static Gson gson = new Gson();
    private static final int eventsNum = 100;

    /**
     * delimiter for separate json objects in the same file
     */
    private static final String delimiter = "\n{'empty':''}\n";

    private String filename;

    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Before public void prepareDataFile() {
        try {
            File file = folder.newFile();
            filename = file.getAbsolutePath();
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter writer = new BufferedWriter(fw);
            for (int i = 0; i < eventsNum; i++) {
                writer.write(DummyEvent.generateJSON());
                writer.write(delimiter );
            }
            writer.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    /**
     * Test possibility to read file with json.
     * 
     * @throws Exception
     */
    @Test public void testInputFileSplitting() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        DataStream<Event> dataStream =
                env.readFile(new DataFileInputFormat(filename), filename)
                        .map(json -> {
                            Event event = gson.fromJson(json, Event.class);
                            return event;
                        })
                        .filter(event -> event != null)
                        .filter(event -> {
                            assertTrue(event.getUnixtimestamp() != null);
                            return true;
                        });

        dataStream.map(event -> event.getTimestamp()).print();
        env.execute();
    }
}
