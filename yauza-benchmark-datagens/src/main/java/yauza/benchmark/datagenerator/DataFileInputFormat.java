package yauza.benchmark.datagenerator;

import org.apache.flink.api.java.io.PrimitiveInputFormat;
import org.apache.flink.core.fs.Path;

/**
 * Simple input splitter.
 * As a delimiter not is used a dummy json object
 *
 */
public class DataFileInputFormat extends PrimitiveInputFormat<String> {

    private static final long serialVersionUID = 8341785562068427225L;
    public static final String delimiter = "{'empty':''}";

    public DataFileInputFormat(String filePath) {
        super(new Path(filePath), delimiter, String.class);
    }
}
