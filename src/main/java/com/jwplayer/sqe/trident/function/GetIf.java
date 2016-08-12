package com.jwplayer.sqe.trident.function;

import org.apache.storm.tuple.Values;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class GetIf extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Boolean predicateValue = (Boolean) tuple.get(0);
        Object trueValue = tuple.get(1);
        Object falseValue = tuple.get(2);

        collector.emit(new Values(predicateValue ? trueValue : falseValue));
    }
}
