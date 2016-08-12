package com.jwplayer.sqe.language.expression.transform.predicate;


public class GreaterThanEqualCompareNumbers extends CompareNumbers {
    public GreaterThanEqualCompareNumbers() {
        super(NumberComparisonType.GreaterThanOrEqual);
    }

    @Override
    public String getFunctionName() {
        return ">=";
    }
}
