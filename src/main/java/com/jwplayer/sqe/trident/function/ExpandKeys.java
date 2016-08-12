package com.jwplayer.sqe.trident.function;

import org.apache.avro.util.Utf8;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.TreeSet;


public class ExpandKeys extends BaseFunction {
    @Override
    @SuppressWarnings("unchecked")
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map map = (Map) tuple.get(0);
        TreeSet set = new TreeSet(map.keySet());

        for(Object key: set) {
            // Avro strings are stored using a special Avro type instead of using Java primitives
            if(key instanceof Utf8) {
                collector.emit(new Values(key.toString()));
            } else {
                collector.emit(new Values(key));
            }
        }
    }
}
