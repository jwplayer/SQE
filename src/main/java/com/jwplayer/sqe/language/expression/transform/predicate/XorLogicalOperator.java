package com.jwplayer.sqe.language.expression.transform.predicate;


public class XorLogicalOperator extends LogicalOperator {
    public XorLogicalOperator() {
        super(LogicalOperatorType.Xor);
    }

    @Override
    public String getFunctionName() {
        return "xor";
    }
}
