package com.jwplayer.sqe.trident.aggregator;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.storm.trident.operation.TridentCollector;

public class HllAggregator extends CardinalityEstimatorAggregator {
    private int log2m;

    public HllAggregator(int log2m) {
        this.log2m = log2m;
    }

    @Override
    public HyperLogLog init(Object o, TridentCollector collector) {
        return new HyperLogLog(log2m);
    }
}
