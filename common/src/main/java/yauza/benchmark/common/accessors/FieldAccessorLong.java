package yauza.benchmark.common.accessors;

import java.io.Serializable;

import yauza.benchmark.common.Event;

public interface FieldAccessorLong extends Serializable {
    Long apply(Event event);
}
