package com.jwplayer.sqe.language.expression.aggregation;

import com.jwplayer.sqe.language.expression.BaseExpression;
import com.jwplayer.sqe.language.expression.FieldExpression;
import com.jwplayer.sqe.language.expression.FunctionExpression;
import com.jwplayer.sqe.language.expression.FunctionType;
import org.apache.storm.trident.fluent.ChainedFullAggregatorDeclarer;
import org.apache.storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;


public abstract class AggregationExpression extends FunctionExpression {
    public AggregationExpression() {
    }

    public abstract ChainedFullAggregatorDeclarer aggregate(ChainedFullAggregatorDeclarer stream,
                                                   Fields inputFields, Fields outputFields);

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.Aggregation;
    }

    public abstract ChainedPartitionAggregatorDeclarer partitionAggregate(ChainedPartitionAggregatorDeclarer stream,
                                                                 Fields inputFields, Fields outputFields);

    public abstract void persistentAggregate(GroupedStream stream, Fields inputFields, StateFactory factory,
                                             Fields functionFields);

    // HACK: We override this for aggregation expressions so we only unroll the first argument so we can keep
    // constants in the 2nd (or 3rd, etc.) positions available to be pulled from the arguments and not the stream.
    // This is needed because aggregators instantiate the object they are using to aggregate before access to the
    // stream. This is particularly problematic with HllCreate, where we need a log2m value to create the HLL
    // object, but want to have this be an argument of the expression. In general, aggregation expressions should
    // only take a single argument, except in cases like this where the 2nd argument seemed a natural position
    // for such a value.
    @Override
    public List<BaseExpression> unRoll() {
        List<BaseExpression> retVal = new ArrayList<>();
        List<BaseExpression> expressionSet = getArguments().get(0).unRoll();

        if(expressionSet != null) {
            expressionSet.add(getArguments().get(0));
            getArguments().set(0, new FieldExpression(getArguments().get(0).getOutputFieldName()));
            retVal.addAll(expressionSet);
        }

        return retVal;
    }
}
