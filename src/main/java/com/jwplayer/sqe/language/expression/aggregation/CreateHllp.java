package com.jwplayer.sqe.language.expression.aggregation;

import com.jwplayer.sqe.trident.aggregator.HllpAggregator;
import com.jwplayer.sqe.trident.aggregator.HllpCombinerAggregator;
import org.apache.storm.trident.fluent.ChainedFullAggregatorDeclarer;
import org.apache.storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;


public class CreateHllp extends CreateHll {
    public CreateHllp() {
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(ChainedFullAggregatorDeclarer stream, Fields inputFields, Fields outputFields) {
        return stream.aggregate(inputFields, new HllpCombinerAggregator(getLog2m()), outputFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(ChainedPartitionAggregatorDeclarer stream, Fields inputFields, Fields outputFields) {
        return stream.partitionAggregate(inputFields, new HllpAggregator(getLog2m()), outputFields);
    }

    @Override
    public void persistentAggregate(GroupedStream stream, Fields inputFields, StateFactory factory, Fields functionFields) {
        stream.persistentAggregate(factory, inputFields, new HllpCombinerAggregator(getLog2m()), functionFields);
    }

    @Override
    public String getFunctionName() {
        return "createhllp";
    }
}
