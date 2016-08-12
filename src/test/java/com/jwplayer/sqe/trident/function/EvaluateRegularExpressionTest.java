package com.jwplayer.sqe.trident.function;

import static org.junit.Assert.*;

import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;


public class EvaluateRegularExpressionTest {
    @Test
    public void testEvaluation() {
        EvaluateRegularExpression evaluator = new EvaluateRegularExpression();
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("Value", "RegEx"), "ABCD1234", "[0-9a-zA-Z]{8}");
        evaluator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("Value", "RegEx"), "ABC_1234", "[0-9a-zA-Z]{8}");
        evaluator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("Value", "RegEx"), "ABCD12345", "[0-9a-zA-Z]{8}");
        evaluator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));
    }
}
