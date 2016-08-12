package com.jwplayer.sqe.trident.function;

import static org.junit.Assert.*;

import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;



public class CompareObjectsTest {
    @Test
    public void testNullComparison() {
        CompareObjects comparator = new CompareObjects();
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), null, 1);
        comparator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), 1, null);
        comparator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), null, null);
        comparator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));
    }

    @Test
    public void testNumberComparison() {
        CompareObjects comparator = new CompareObjects();
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), 1, 1.0f);
        comparator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), 1, 2);
        comparator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), 1, "Cat");
        comparator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));
    }

    @Test
    public void testObjectComparison() {
        CompareObjects comparator = new CompareObjects();
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), comparator, comparator);
        comparator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), comparator, new CompareObjects());
        comparator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));
    }

    @Test
    public void testStringComparison() {
        CompareObjects comparator = new CompareObjects();
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), "Cat", "Cat");
        comparator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("S1", "S2"), "Cat", "Dog");
        comparator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));
    }
}
