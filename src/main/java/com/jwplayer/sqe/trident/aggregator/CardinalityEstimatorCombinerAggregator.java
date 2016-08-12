package com.jwplayer.sqe.trident.aggregator;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;


/* This aggregator is for taking existing cardinality estimator bitmaps/objects and aggregating/combining them. */
public abstract class CardinalityEstimatorCombinerAggregator implements CombinerAggregator<byte[]> {
    @Override
    public byte[] init(TridentTuple tuple) {
        return (byte[]) tuple.getValue(0);
    }
}
