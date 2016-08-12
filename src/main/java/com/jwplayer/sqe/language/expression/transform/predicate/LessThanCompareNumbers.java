package com.jwplayer.sqe.language.expression.transform.predicate;


public class LessThanCompareNumbers extends CompareNumbers {
    public LessThanCompareNumbers() {
        super(NumberComparisonType.LessThan);
    }

    @Override
    public String getFunctionName() {
        return "<";
    }
}
