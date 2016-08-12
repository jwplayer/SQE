package com.jwplayer.sqe.trident.function;

import org.apache.storm.tuple.Values;
import clojure.lang.Numbers;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class IsIn extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if(tuple.size() < 2) throw new RuntimeException("IfIn function expects more than 1 value on the tuple");

        Object value = tuple.get(0);
        Boolean valueFound = false;

        if(value instanceof Number) {
            Number number = (Number) value;

            for(int i = 1; i < tuple.size(); i++) {
                if(Numbers.equiv(number, (Number) tuple.get(i))) valueFound = true;
            }
        } else {
            for(int i = 1; i < tuple.size(); i++) {
                if(value.equals(tuple.get(i))) valueFound = true;
            }
        }

        collector.emit(new Values(valueFound));
    }
}
