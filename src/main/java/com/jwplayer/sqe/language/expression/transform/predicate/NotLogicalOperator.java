package com.jwplayer.sqe.language.expression.transform.predicate;


public class NotLogicalOperator extends LogicalOperator {
    public NotLogicalOperator() {
        super(LogicalOperatorType.Not);
    }

    @Override
    public String getFunctionName() {
        return "not";
    }
}
