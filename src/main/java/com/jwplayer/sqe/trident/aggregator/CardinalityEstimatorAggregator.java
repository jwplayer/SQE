package com.jwplayer.sqe.trident.aggregator;

import org.apache.storm.tuple.Values;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.io.IOException;


/* This aggregator is for creating new cardinality estimators and efficiently adding fields you want
 * to count. Once the object is created, you can use the associated CombinerAggregator for combing multiple
 * objects into a single bitmap. */
public abstract class CardinalityEstimatorAggregator extends BaseAggregator<ICardinality> {
    @Override
    public void aggregate(ICardinality cardinalityEstimator, TridentTuple tuple, TridentCollector collector) {
        // Replicate SQL COUNT(DISTINCT) functionality to not count NULLs as a countable value
        if(tuple.getValue(0) != null) cardinalityEstimator.offer(tuple.getValue(0));
    }

    @Override
    public void complete(ICardinality cardinalityEstimator, TridentCollector collector) {
        try {
            collector.emit(new Values(cardinalityEstimator.getBytes()));
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}