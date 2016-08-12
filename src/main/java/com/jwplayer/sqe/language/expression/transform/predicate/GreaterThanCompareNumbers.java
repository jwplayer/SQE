package com.jwplayer.sqe.language.expression.transform.predicate;


public class GreaterThanCompareNumbers extends CompareNumbers {
    public GreaterThanCompareNumbers() {
        super(NumberComparisonType.GreaterThan);
    }

    @Override
    public String getFunctionName() {
        return ">";
    }
}
