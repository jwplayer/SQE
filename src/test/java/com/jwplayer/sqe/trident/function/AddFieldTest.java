package com.jwplayer.sqe.trident.function;

import static org.junit.Assert.*;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.util.Arrays;


public class AddFieldTest {
    @Test
    public void testAddInteger() {
        AddField addField = new AddField(1);
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields());
        SingleValuesCollector collector = new SingleValuesCollector();

        addField.execute(tuple, collector);

        assertEquals(collector.values.get(0), 1);
    }

    @Test
    public void testAddString() {
        AddField addField = new AddField("Abc");
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields());
        SingleValuesCollector collector = new SingleValuesCollector();

        addField.execute(tuple, collector);

        assertEquals(collector.values.get(0), "Abc");
    }

    @Test
    public void testAddBitmap() throws IOException {
        HyperLogLog hll = new HyperLogLog(9);
        hll.offer("a");
        hll.offer("b");
        hll.offer("C");
        AddField addField = new AddField(hll.getBytes());
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields());
        SingleValuesCollector collector = new SingleValuesCollector();

        addField.execute(tuple, collector);
        assertTrue(Arrays.equals((byte[]) collector.values.get(0), hll.getBytes()));
    }
}
