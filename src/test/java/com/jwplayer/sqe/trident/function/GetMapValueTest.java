package com.jwplayer.sqe.trident.function;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import com.jwplayer.sqe.trident.SingleValuesCollector;


public class GetMapValueTest {
    @Test
    public void testGetMapValue() {
        GetMapValue getMapValueFromKey = new GetMapValue();
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(123, "S");
        map.put(456, "T");
        
        TridentTuple tuple1 = TridentTupleView.createFreshTuple(new Fields("map", "key"), map, 123);
        TridentTuple tuple2 = TridentTupleView.createFreshTuple(new Fields("map", "key"), map, 456);

        SingleValuesCollector collector = new SingleValuesCollector();
        getMapValueFromKey.execute(tuple1, collector);
        assertEquals(collector.values.get(0), "S");
        
        getMapValueFromKey.execute(tuple2, collector);
        assertEquals(collector.values.get(0), "T");
    }

}
