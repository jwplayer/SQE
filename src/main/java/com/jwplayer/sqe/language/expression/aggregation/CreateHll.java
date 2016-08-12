package com.jwplayer.sqe.language.expression.aggregation;

import com.jwplayer.sqe.language.expression.BaseExpression;
import com.jwplayer.sqe.language.expression.ConstantExpression;
import com.jwplayer.sqe.language.expression.ExpressionType;
import com.jwplayer.sqe.trident.aggregator.HllAggregator;
import com.jwplayer.sqe.trident.aggregator.HllCombinerAggregator;
import org.apache.storm.trident.fluent.ChainedFullAggregatorDeclarer;
import org.apache.storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;


public class CreateHll extends AggregationExpression {
    public CreateHll() {
    }

    protected int getLog2m() {
        if(getArguments().size() == 2) {
            BaseExpression expression = getArguments().get(1);
            if(expression.getExpressionType() == ExpressionType.Constant) {
                ConstantExpression cExp = (ConstantExpression) expression;
                if(cExp.constant instanceof Number) {
                    return ((Number) cExp.constant).intValue();
                } else {
                    throw new RuntimeException(String.format("Optional 2nd argument to %s must be a number", getFunctionName()));
                }
            } else {
                throw new RuntimeException(String.format("Optional 2nd argument to %s must be a constant", getFunctionName()));
            }
        } else {
            return 11; // Default log2m
        }
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(ChainedFullAggregatorDeclarer stream, Fields inputFields, Fields outputFields) {
        return stream.aggregate(inputFields, new HllCombinerAggregator(getLog2m()), outputFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(ChainedPartitionAggregatorDeclarer stream, Fields inputFields, Fields outputFields) {
        return stream.partitionAggregate(inputFields, new HllAggregator(getLog2m()), outputFields);
    }

    @Override
    public void persistentAggregate(GroupedStream stream, Fields inputFields, StateFactory factory, Fields functionFields) {
        stream.persistentAggregate(factory, inputFields, new HllCombinerAggregator(getLog2m()), functionFields);
    }

    @Override
    public String getFunctionName() {
        return "createhll";
    }
}
