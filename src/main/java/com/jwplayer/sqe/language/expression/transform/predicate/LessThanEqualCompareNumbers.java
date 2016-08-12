package com.jwplayer.sqe.language.expression.transform.predicate;


public class LessThanEqualCompareNumbers extends CompareNumbers {
    public LessThanEqualCompareNumbers() {
        super(NumberComparisonType.LessThanOrEqual);
    }

    @Override
    public String getFunctionName() {
        return "<=";
    }
}
