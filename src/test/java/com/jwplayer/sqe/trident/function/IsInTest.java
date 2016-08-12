package com.jwplayer.sqe.trident.function;

import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import static org.junit.Assert.*;


public class IsInTest {
    @Test
    public void testIsInNumbers() {
        IsIn isIn = new IsIn();
        SingleValuesCollector collector = new SingleValuesCollector();

        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("A", "1", "2"), 1.0f, 1.1d, 2l);
        isIn.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("A", "1", "2", "3"), 1.0f, 1.1d, 2l, 1);
        isIn.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));
    }

    @Test
    public void testIsInString() {
        IsIn isIn = new IsIn();
        SingleValuesCollector collector = new SingleValuesCollector();

        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("A", "1", "2"), "A", "cat", "moo");
        isIn.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("A", "1", "2", "3"), "A", "cat", "A", "moo");
        isIn.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));
    }
}
