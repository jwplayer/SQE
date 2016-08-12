package com.jwplayer.sqe.language.expression.transform.predicate;


public class AndLogicalOperator extends LogicalOperator {
    public AndLogicalOperator() {
        super(LogicalOperatorType.And);
    }

    @Override
    public String getFunctionName() {
        return "and";
    }
}
