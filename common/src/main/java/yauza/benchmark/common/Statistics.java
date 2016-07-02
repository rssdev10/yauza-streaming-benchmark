package yauza.benchmark.common;

import java.io.Serializable;

/**
 */
public class Statistics implements Serializable {
    public long firstTime;
    public long lastTime;
    public long count;

    public void registerEvent(Event event) {
        lastTime = event.getInputTime();
        if (firstTime == 0)
            firstTime = lastTime;
        count += 1;
    }

    public void summarize(Statistics other) {
        if (firstTime == 0) {
            firstTime = other.firstTime;
        } else {
            firstTime = Math.min(firstTime, other.firstTime);
        }
        lastTime = Math.max(lastTime, other.lastTime);
        count += other.count;
    }
}
