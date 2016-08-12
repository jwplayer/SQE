package com.jwplayer.sqe.language.expression.aggregation;

import com.jwplayer.sqe.trident.aggregator.GetMaximumAggregator;
import org.apache.storm.trident.fluent.ChainedFullAggregatorDeclarer;
import org.apache.storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;


public class Max extends AggregationExpression {
    public Max() {
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(ChainedFullAggregatorDeclarer stream, Fields inputFields, Fields outputFields) {
        return stream.aggregate(inputFields, (CombinerAggregator) new GetMaximumAggregator(), outputFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(ChainedPartitionAggregatorDeclarer stream, Fields inputFields, Fields outputFields) {
        return stream.partitionAggregate(inputFields, (Aggregator) new GetMaximumAggregator(), outputFields);
    }

    @Override
    public void persistentAggregate(GroupedStream stream, Fields inputFields, StateFactory factory, Fields functionFields) {
        stream.persistentAggregate(factory, inputFields, new GetMaximumAggregator(), functionFields);
    }

    @Override
    public String getFunctionName() {
        return "max";
    }
}
