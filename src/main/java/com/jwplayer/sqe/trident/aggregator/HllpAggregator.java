package com.jwplayer.sqe.trident.aggregator;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.storm.trident.operation.TridentCollector;


public class HllpAggregator extends CardinalityEstimatorAggregator {
    private int log2m;

    public HllpAggregator(int log2m) {
        this.log2m = log2m;
    }

    @Override
    public HyperLogLogPlus init(Object o, TridentCollector collector) {
        return new HyperLogLogPlus(log2m, log2m);
    }
}
