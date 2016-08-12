package com.jwplayer.sqe.trident.aggregator;

import static org.junit.Assert.*;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Before;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import java.io.IOException;


public class HllpAggregatorTest {
    private CardinalityEstimatorAggregator aggregator;
    private SingleValuesCollector collector;

    @Before
    public void setup() {
        aggregator = new HllpAggregator(11);
        collector = new SingleValuesCollector();
    }

    @Test
    public void testAggregator() throws IOException {
        ICardinality hllp = aggregator.init(null, collector);

        assertEquals(hllp.cardinality(), 0);

        aggregator.aggregate(hllp, TridentTupleView.createFreshTuple(new Fields("Object"), "a"), collector);
        aggregator.aggregate(hllp, TridentTupleView.createFreshTuple(new Fields("Object"), "b"), collector);
        aggregator.aggregate(hllp, TridentTupleView.createFreshTuple(new Fields("Object"), "c"), collector);

        assertEquals(hllp.cardinality(), 3);

        aggregator.complete(hllp, collector);
        HyperLogLogPlus hllp2 = HyperLogLogPlus.Builder.build((byte[]) collector.values.get(0));

        assertEquals(hllp.cardinality(), hllp2.cardinality());
    }
}
