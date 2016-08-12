package com.jwplayer.sqe.trident.function;

import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import static org.junit.Assert.assertEquals;

public class GetIfTest {
    @Test
    public void testFalsePredicate() {
        GetIf getIf = new GetIf();
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("Predicate", "TrueValue", "FalseValue"), false, "a", "b");
        SingleValuesCollector collector = new SingleValuesCollector();

        getIf.execute(tuple, collector);
        assertEquals(collector.values.get(0), "b");
    }

    @Test
    public void testTruePredicate() {
        GetIf getIf = new GetIf();
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("Predicate", "TrueValue", "FalseValue"), true, "a", "b");
        SingleValuesCollector collector = new SingleValuesCollector();

        getIf.execute(tuple, collector);
        assertEquals(collector.values.get(0), "a");
    }
}
