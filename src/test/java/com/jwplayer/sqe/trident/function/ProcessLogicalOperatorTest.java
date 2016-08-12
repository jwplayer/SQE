package com.jwplayer.sqe.trident.function;

import static org.junit.Assert.*;

import com.jwplayer.sqe.language.expression.transform.predicate.LogicalOperatorType;
import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;


public class ProcessLogicalOperatorTest {
    @Test
    public void testAnd() {
        ProcessLogicalOperator operator = new ProcessLogicalOperator(LogicalOperatorType.And);
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), true, true);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), true, false);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), false, true);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), false, false);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("P1", "P2", "P3"), true, true, true);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("P1", "P2", "P3"), true, true, false);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));
    }

    @Test
    public void testNot() {
        ProcessLogicalOperator operator = new ProcessLogicalOperator(LogicalOperatorType.Not);
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("Predicate"), true);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("Predicate"), false);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));
    }

    @Test
    public void testOr() {
        ProcessLogicalOperator operator = new ProcessLogicalOperator(LogicalOperatorType.Or);
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), true, true);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), true, false);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), false, true);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), false, false);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("P1", "P2", "P3"), true, false, true);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("P1", "P2", "P3"), false, false, false);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));
    }

    @Test
    public void testXor() {
        ProcessLogicalOperator operator = new ProcessLogicalOperator(LogicalOperatorType.Xor);
        TridentTuple tuple;
        SingleValuesCollector collector = new SingleValuesCollector();

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), true, true);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), true, false);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), false, true);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("LeftPredicate", "RightPredicate"), false, false);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("P1", "P2", "P3"), true, true, true);
        operator.execute(tuple, collector);
        assertTrue((Boolean) collector.values.get(0));

        tuple = TridentTupleView.createFreshTuple(new Fields("P1", "P2", "P3"), true, false, true);
        operator.execute(tuple, collector);
        assertFalse((Boolean) collector.values.get(0));
    }
}
