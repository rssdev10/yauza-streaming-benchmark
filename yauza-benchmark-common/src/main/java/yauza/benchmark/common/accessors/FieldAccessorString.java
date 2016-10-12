package yauza.benchmark.common.accessors;

import java.io.Serializable;

import yauza.benchmark.common.Event;

public interface FieldAccessorString extends Serializable {
    String apply(Event event);
}
