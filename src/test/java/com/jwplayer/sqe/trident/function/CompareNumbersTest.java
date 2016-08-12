package com.jwplayer.sqe.trident.function;

import static org.junit.Assert.*;

import com.jwplayer.sqe.language.expression.transform.predicate.NumberComparisonType;
import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;


public class CompareNumbersTest {
    @Test
    public void testEquals() {
        performTests(new CompareNumbers(NumberComparisonType.Equal), true);
    }

    @Test
    public void testGreaterThan() {
        performTests(new CompareNumbers(NumberComparisonType.GreaterThan), false);
    }

    @Test
    public void testGreaterThanOrEqual() {
        performTests(new CompareNumbers(NumberComparisonType.GreaterThanOrEqual), true);
    }

    @Test
    public void testLessThan() {
        performTests(new CompareNumbers(NumberComparisonType.LessThan), false);
    }

    @Test
    public void testLessThanOrEqual() {
        performTests(new CompareNumbers(NumberComparisonType.LessThanOrEqual), true);
    }

    @Test
    public void testNotEqual() {
        performTests(new CompareNumbers(NumberComparisonType.NotEqual), false);
    }

    private void performTests(CompareNumbers compareNumbers, Boolean assertTrue) {
        SingleValuesCollector collector = new SingleValuesCollector();

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1, 1), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1, 1l), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1, 1.0f), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1, 1.0d), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1l, 1l), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1l, 1.0f), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1l, 1.0d), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1.0f, 1.0f), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1.0f, 1.0d), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));

        compareNumbers.execute(TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 1.0d, 1.0d), collector);
        assertTrue(assertTrue ? (Boolean) collector.values.get(0) : !(Boolean) collector.values.get(0));
    }
}
