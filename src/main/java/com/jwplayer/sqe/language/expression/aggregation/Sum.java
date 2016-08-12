package com.jwplayer.sqe.language.expression.aggregation;

import org.apache.storm.trident.fluent.ChainedFullAggregatorDeclarer;
import org.apache.storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;


public class Sum extends AggregationExpression {
    public Sum() {
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(ChainedFullAggregatorDeclarer stream, Fields inputFields, Fields outputFields) {
        return stream.aggregate(inputFields, new org.apache.storm.trident.operation.builtin.Sum(), outputFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(ChainedPartitionAggregatorDeclarer stream, Fields inputFields, Fields outputFields) {
        return stream.partitionAggregate(inputFields, new org.apache.storm.trident.operation.builtin.Sum(), outputFields);
    }

    @Override
    public void persistentAggregate(GroupedStream stream, Fields inputFields, StateFactory factory, Fields functionFields) {
        stream.persistentAggregate(factory, inputFields, new org.apache.storm.trident.operation.builtin.Sum(), functionFields);
    }

    @Override
    public String getFunctionName() {
        return "sum";
    }
}
