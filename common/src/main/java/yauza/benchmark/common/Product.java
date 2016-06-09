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

    private Long unixTimestamp;

    /** latency in ms */
    private Long latency;
    /** all time of processing in ms */
    private Long processingTime;
    /** number of processed events */
    private Long processedEvents;

    public Product(String value) {
        this.unixTimestamp = new Date().getTime();
        this.value = value;
        this.timestamp = sdf.format(unixTimestamp);
    }

    public Product(String type, String value) {
        this.unixTimestamp = new Date().getTime();
        this.type = type;
        this.value = value;
        this.timestamp = sdf.format(unixTimestamp);
    }

    public Product(String timestamp, String type, String value) {
        this.unixTimestamp = new Date().getTime();
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

    public Long getLatency() {
        return latency;
    }

    public void setLatency(Long latency) {
        this.latency = latency;
    }

    public Long getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(Long processingTime) {
        this.processingTime = processingTime;
    }

    public Long getProcessedEvents() {
        return processedEvents;
    }

    public void setProcessedEvents(Long processedEvents) {
        this.processedEvents = processedEvents;
    }

    public void setStatistics(Statistics value){
        this.latency = unixTimestamp - value.lastTime;
        this.processingTime = unixTimestamp - value.firstTime;
        this.processedEvents = value.count;
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }
}
