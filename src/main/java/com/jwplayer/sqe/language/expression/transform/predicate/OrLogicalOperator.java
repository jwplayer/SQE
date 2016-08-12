package com.jwplayer.sqe.language.expression.transform.predicate;


public class OrLogicalOperator extends LogicalOperator {
    public OrLogicalOperator() {
        super(LogicalOperatorType.Or);
    }

    @Override
    public String getFunctionName() {
        return "or";
    }
}
