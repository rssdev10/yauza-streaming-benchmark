package yauza.benchmark.common;

import com.google.gson.Gson;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 */
public class Product {
    final static private SimpleDateFormat sdf = new SimpleDateFormat(Event.eventTimeFormat);
    final static private Gson gson = new Gson();

    private String timestamp;
    private String type;
    private String value;

    public Product(String value) {
        this.value = value;
        this.timestamp = sdf.format(new Date().getTime());
    }

    public Product(String type, String value) {
        this.type = type;
        this.value = value;
        this.timestamp = sdf.format(new Date().getTime());
    }

    public Product(String timestamp, String type, String value) {
        this.timestamp = timestamp;
        this.type = type;
        this.value = value;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }
}
