package yauza.benchmark.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Trigger} that fires on items count or on processing time.
 *
 * @param <W> The type of {@link Time Window Windows} on which this trigger can operate.
 */
public class CountOrTimeTrigger<W extends TimeWindow> extends Trigger<Object, W> {

    private static final long serialVersionUID = 3088194039662578933L;

    Logger LOG = LoggerFactory.getLogger(CountOrTimeTrigger.class);
    
    private final long maxCount;

    private final ValueStateDescriptor<Long> stateDesc =
            new ValueStateDescriptor<>("count", LongSerializer.INSTANCE, 0L);

    private CountOrTimeTrigger(long count) {
        this.maxCount = count;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ctx.registerProcessingTimeTimer(window.getEnd());

        ValueState<Long> count = ctx.getPartitionedState(stateDesc);
        long currentCount = count.value() + 1;
        count.update(currentCount);
        if (currentCount >= maxCount) {
            count.update(0L);
//            LOG.debug("onElement");
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
//      LOG.debug("onProcessingTime");
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).clear();
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    /**
     * Creates a trigger that fires on item count or on processing time.
     *
     * @param count of items to fire.
     * @param <W> The type of {@link TimeWindow Windows} on which this trigger can operate.
     */
    public static <W extends TimeWindow> CountOrTimeTrigger<W> of(long count) {
        return new CountOrTimeTrigger<>(count);
    }
}