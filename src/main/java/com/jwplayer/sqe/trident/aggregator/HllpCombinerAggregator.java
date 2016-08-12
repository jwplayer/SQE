package com.jwplayer.sqe.trident.aggregator;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import java.io.IOException;

public class HllpCombinerAggregator extends CardinalityEstimatorCombinerAggregator {
    private int log2m;

    public HllpCombinerAggregator(int log2m) {
        this.log2m = log2m;
    }

    @Override
    public byte[] combine(byte[] bitmap1, byte[] bitmap2) {
        try {
            HyperLogLogPlus hllp1 = HyperLogLogPlus.Builder.build(bitmap1);
            HyperLogLogPlus hllp2 = HyperLogLogPlus.Builder.build(bitmap2);
            hllp1.addAll(hllp2);
            return hllp1.getBytes();
        } catch (CardinalityMergeException |IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public byte[] zero() {
        try {
            return (new HyperLogLogPlus(log2m, log2m)).getBytes();
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
