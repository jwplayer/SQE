package com.jwplayer.sqe.trident.function;

import org.apache.storm.tuple.Values;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class AddField extends BaseFunction {
    private Object value;

    public AddField(Object value) {
        this.value = value;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(value));
    }
}
