package com.jwplayer.sqe.trident.function;

import java.util.Map;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class GetMapValue extends BaseFunction{

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // TODO Auto-generated method stub
        @SuppressWarnings("unchecked")
        Map<Object, Object> m = (Map<Object, Object>) tuple.get(0);
        Object key = tuple.get(1);

        if (m.containsKey(key)){
            collector.emit(new Values(m.get(key)));
        }
    }

}
