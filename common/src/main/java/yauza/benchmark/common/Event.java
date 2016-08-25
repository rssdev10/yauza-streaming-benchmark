package yauza.benchmark.common;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
  Class for manipulation with universal event. This class presents absract
  event structure about current app's activity from mobile devices to web server
*/
public class Event implements Serializable {
    public static final String eventTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    private String timestamp;

    private Long unixtimestamp;

    private String deviceId;
    private String vendor;
    private String device;
    private String osType;
    private String osVer;

    private String ip;
    private String country = "";

    private String userId = "";

    private String sessionId = "";

    private String receiptId;
    private String provider;
    private Long price;

    /** actual time of the event arrival */
    private Long inputTime;

    public final String getTimestamp() {
        return timestamp;
    }

    public final void setTimestamp(String timestamp) {
        this.timestamp = timestamp;

        final SimpleDateFormat sdf = new SimpleDateFormat(eventTimeFormat);
        try {
            this.unixtimestamp = sdf.parse(timestamp).getTime();
        } catch (ParseException e) {
            this.unixtimestamp = 0l;
        }
    }

    public final String getDeviceId() {
        return deviceId;
    }

    public final void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public final String getVendor() {
        return vendor;
    }

    public final void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public final String getDevice() {
        return device;
    }

    public final void setDevice(String device) {
        this.device = device;
    }

    public final String getOsType() {
        return osType;
    }

    public final void setOsType(String osType) {
        this.osType = osType;
    }

    public final String getOsVer() {
        return osVer;
    }

    public final void setOsVer(String osVer) {
        this.osVer = osVer;
    }

    public final String getIp() {
        return ip;
    }

    public final void setIp(String ip) {
        this.ip = ip;
    }

    public final String getCountry() {
        return country;
    }

    public final void setCountry(String country) {
        this.country = country;
    }

    public final String getUserId() {
        return userId;
    }

    public final void setUserId(String userId) {
        this.userId = userId;
    }

    public final String getSessionId() {
        return sessionId;
    }

    public final void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public final String getReceiptId() {
        return receiptId;
    }

    public final void setReceiptId(String receiptId) {
        this.receiptId = receiptId;
    }

    public final String getProvider() {
        return provider;
    }

    public final void setProvider(String provider) {
        this.provider = provider;
    }

    public final Long getPrice() {
        return price;
    }

    public final void setPrice(Long price) {
        this.price = price;
    }

    public final Long getUnixtimestamp() {
        return unixtimestamp;
    }

    public final void setUnixtimestamp(Long unixtimestamp) {
        this.unixtimestamp = unixtimestamp;
    }

    public Long getInputTime() {
        return inputTime;
    }

    public void setInputTime() {
        inputTime = new Date().getTime();
    }

    public void setInputTime(Long inputTime) {
        this.inputTime = inputTime;
    }

    public int partition(int totalPartitions) {
        return Math.abs(this.userId.hashCode()) % totalPartitions;
    }
}
