package com.jwplayer.sqe.trident.aggregator;

import clojure.lang.Numbers;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class GetMaximumAggregator extends BaseAggregator<GetMaximumAggregator.MaximumState> implements CombinerAggregator<Comparable> {
    public static class MaximumState {
        Comparable comparable;

        public MaximumState(Comparable comparable) {
            this.comparable = comparable;
        }
    }

    @Override
    public GetMaximumAggregator.MaximumState init(Object txID, TridentCollector collector) {
        return new MaximumState(null);
    }

    @Override
    public void aggregate(GetMaximumAggregator.MaximumState state, TridentTuple tuple, TridentCollector collector) {
        if(tuple.get(0) instanceof Comparable) {
            state.comparable = combine(state.comparable, (Comparable) tuple.get(0));
        } else {
            throw new RuntimeException(
                    String.format("All values sent to the Max function must implement Comparable<T>. The given value's type is %s.",
                            tuple.get(0).getClass().toString()));
        }
    }

    @Override
    public void complete(GetMaximumAggregator.MaximumState state, TridentCollector collector) {
        collector.emit(new Values(state.comparable));
    }

    @Override
    public Comparable init(TridentTuple tuple) {
        if(tuple.get(0) instanceof Comparable) {
            return (Comparable) tuple.get(0);
        } else {
            throw new RuntimeException(
                    String.format("All values sent to the Max function must implement Comparable<T>. The given value's type is %s.",
                            tuple.get(0).getClass().toString()));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Comparable combine(Comparable comp1, Comparable comp2) {
        if(comp1 == null) {
            return comp2;
        } else if(comp2 == null) {
            return comp1;
        } else if(comp1 instanceof Number && comp2 instanceof Number) {
            // Handle Numbers specially so we can compare cross type
            return (Comparable) Numbers.max(comp1, comp2);
        } else if(comp1.compareTo(comp2) < 0) {
            return comp2;
        } else {
            return comp1;
        }
    }

    @Override
    public Comparable zero() {
        return null;
    }
}
