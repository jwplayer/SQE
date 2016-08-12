package com.jwplayer.sqe.trident.function;

import static org.junit.Assert.*;

import com.jwplayer.sqe.language.expression.transform.ArithmeticOperatorType;
import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;


public class ProcessArithmeticOperatorTest {
    @Test
    public void testAddition() {
        ProcessArithmeticOperator operator = new ProcessArithmeticOperator(ArithmeticOperatorType.Addition);
        SingleValuesCollector collector = new SingleValuesCollector();

        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0f, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0f, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0d, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);
    }

    @Test
    public void testDivision() {
        ProcessArithmeticOperator operator = new ProcessArithmeticOperator(ArithmeticOperatorType.Division);
        SingleValuesCollector collector = new SingleValuesCollector();

        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 4, 2);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5, 2);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 4, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 4l, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5l, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5l, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5l, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5.0f, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5.0f, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5.0d, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 2.5d);
    }

    @Test
    public void testModulus() {
        ProcessArithmeticOperator operator = new ProcessArithmeticOperator(ArithmeticOperatorType.Modulus);
        SingleValuesCollector collector = new SingleValuesCollector();

        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5, 2);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5l, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5l, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5l, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5.0f, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5.0f, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 5.0d, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 1.0d);
    }

    @Test
    public void testMultiplication() {
        ProcessArithmeticOperator operator = new ProcessArithmeticOperator(ArithmeticOperatorType.Multiplication);
        SingleValuesCollector collector = new SingleValuesCollector();

        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0f, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0f, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0d, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 4.0d);
    }

    @Test
    public void testSubtraction() {
        ProcessArithmeticOperator operator = new ProcessArithmeticOperator(ArithmeticOperatorType.Subtraction);
        SingleValuesCollector collector = new SingleValuesCollector();

        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2l);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0l);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2l, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0f, 2.0f);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0f, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0.0d);

        tuple = TridentTupleView.createFreshTuple(new Fields("num1", "num2"), 2.0d, 2.0d);
        operator.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0.0d);
    }
}
