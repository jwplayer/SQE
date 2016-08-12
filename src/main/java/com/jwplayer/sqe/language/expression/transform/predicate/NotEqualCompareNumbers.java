package com.jwplayer.sqe.language.expression.transform.predicate;


public class NotEqualCompareNumbers extends CompareNumbers {
    public NotEqualCompareNumbers() {
        super(NumberComparisonType.NotEqual);
    }

    @Override
    public String getFunctionName() {
        return "!=";
    }
}
