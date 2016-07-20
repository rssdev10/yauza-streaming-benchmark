package yauza.benchmark.datagenerator;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yauza.benchmark.common.helpers.DummyEvent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class provides generation of events and writes them into HDFS
 */
public class HdfsWriter {
    Logger LOG = LoggerFactory.getLogger(HdfsWriter.class);
    public static final int eventsNum = 1000 * 1000 * 10;
//    public static final int eventsNum = 1000 * 10;

    private String written = "0";

    public void generate(String hdfsPath, String dataFile, Long messagesNumber) {
        Configuration configuration = new Configuration();

        System.out.println("Generating " + messagesNumber.toString() + " messages");

        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(new URI(hdfsPath), configuration);
            Path file = new Path(hdfsPath + dataFile);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            OutputStream os = hdfs.create(file,
                    new Progressable() {
                        public void progress() {
                            System.out.println("...messages written: [ " + written + " ]");
                        }
                    });
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            String formattedDelimiter = "\n" + DataFileInputFormat.delimiter;
            for (int i = 0; i < messagesNumber; i++) {
                String element = DummyEvent.generateJSON();
                br.write(element);
                br.write(formattedDelimiter);
                written = Integer.toString(i + 1);
            }

            br.close();
            hdfs.close();

        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
