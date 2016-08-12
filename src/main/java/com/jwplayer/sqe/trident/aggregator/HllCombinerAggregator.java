package com.jwplayer.sqe.trident.aggregator;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.IOException;


public class HllCombinerAggregator extends CardinalityEstimatorCombinerAggregator {
    private int log2m;

    public HllCombinerAggregator(int log2m) {
        this.log2m = log2m;
    }

    @Override
    public byte[] combine(byte[] bitmap1, byte[] bitmap2) {
        try {
            HyperLogLog hll1 = HyperLogLog.Builder.build(bitmap1);
            HyperLogLog hll2 = HyperLogLog.Builder.build(bitmap2);
            hll1.addAll(hll2);
            return hll1.getBytes();
        } catch (CardinalityMergeException |IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public byte[] zero() {
        try {
            return (new HyperLogLog(log2m)).getBytes();
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
