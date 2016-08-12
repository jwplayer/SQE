package com.jwplayer.sqe.trident.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Before;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class GetCardinalityEstimationTest {
    GetCardinalityEstimation estimator = null;
    HyperLogLog hll = null;

    @Before
    public void setup() {
        estimator = new GetCardinalityEstimation();
        hll = new HyperLogLog(11);
        hll.offer("a");
        hll.offer("b");
        hll.offer("C");
    }

    @Test
    public void testHyperLogLogEstimation() {
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("hll"), hll);
        SingleValuesCollector collector = new SingleValuesCollector();

        estimator.execute(tuple, collector);

        assertEquals(collector.values.get(0), 3l);
    }

    @Test
    public void testBitmapEstimation() throws IOException {
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("hll"), hll.getBytes());
        SingleValuesCollector collector = new SingleValuesCollector();

        estimator.execute(tuple, collector);

        assertEquals(collector.values.get(0), 3l);
    }
}
