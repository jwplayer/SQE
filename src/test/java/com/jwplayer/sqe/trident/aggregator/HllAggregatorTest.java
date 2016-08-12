package com.jwplayer.sqe.trident.aggregator;

import static org.junit.Assert.*;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Before;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import java.io.IOException;


public class HllAggregatorTest {
    private CardinalityEstimatorAggregator aggregator;
    private SingleValuesCollector collector;

    @Before
    public void setup() {
        aggregator = new HllAggregator(11);
        collector = new SingleValuesCollector();
    }

    @Test
    public void testAggregator() throws IOException {
        ICardinality hll = aggregator.init(null, collector);

        assertEquals(hll.cardinality(), 0);

        aggregator.aggregate(hll, TridentTupleView.createFreshTuple(new Fields("Object"), "a"), collector);
        aggregator.aggregate(hll, TridentTupleView.createFreshTuple(new Fields("Object"), "b"), collector);
        aggregator.aggregate(hll, TridentTupleView.createFreshTuple(new Fields("Object"), "c"), collector);

        assertEquals(hll.cardinality(), 3);

        aggregator.complete(hll, collector);
        HyperLogLog hll2 = HyperLogLog.Builder.build((byte[]) collector.values.get(0));

        assertEquals(hll.cardinality(), hll2.cardinality());
    }
}
