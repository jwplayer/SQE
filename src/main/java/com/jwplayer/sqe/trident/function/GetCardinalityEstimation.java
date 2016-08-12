package com.jwplayer.sqe.trident.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;


public class GetCardinalityEstimation extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Object object = tuple.get(0);
        HyperLogLog hll;

        if(object instanceof HyperLogLog) {
            hll = (HyperLogLog) object;
        }
        else if(object instanceof byte[]) {
            try {
                hll = HyperLogLog.Builder.build((byte[]) object);
            } catch(IOException ex) {
                throw new RuntimeException("Could not build HyperLogLog object from byte array", ex);
            }
        } else {
            throw new RuntimeException(object.getClass() + " is not an appropriate HLL object or byte array");
        }

        collector.emit(new Values(hll.cardinality()));
    }
}