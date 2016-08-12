package com.jwplayer.sqe.trident.function;

import com.jwplayer.sqe.trident.ListValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;


import static org.junit.Assert.*;

public class ExpandValuesTest {
    @Test
    public void testExpand() {
        ExpandValues expandValues = new ExpandValues();
        Map<String, Integer> map = new HashMap<>();

        map.put("A", 1);
        map.put("B", 2);
        map.put("C", 3);
        map.put("D", 4);

        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("Map"), map);
        ListValuesCollector collector = new ListValuesCollector();

        expandValues.execute(tuple, collector);

        assertEquals(collector.values.size(), 4);
        assertEquals(collector.values.get(0).get(0), 1);
        assertEquals(collector.values.get(1).get(0), 2);
        assertEquals(collector.values.get(2).get(0), 3);
        assertEquals(collector.values.get(3).get(0), 4);
    }
}
