package com.jwplayer.sqe.language.expression.transform.predicate;

import com.jwplayer.sqe.trident.function.ProcessLogicalOperator;
import org.apache.storm.trident.Stream;


public abstract class LogicalOperator extends PredicateExpression {
    LogicalOperatorType operatorType;

    public LogicalOperator(LogicalOperatorType operatorType) {
        super();
        this.operatorType = operatorType;
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new ProcessLogicalOperator(operatorType), getOutputFields());
    }
}
