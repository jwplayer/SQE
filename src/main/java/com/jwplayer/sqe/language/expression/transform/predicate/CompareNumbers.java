package com.jwplayer.sqe.language.expression.transform.predicate;

import org.apache.storm.trident.Stream;


public abstract class CompareNumbers extends PredicateExpression {
    private NumberComparisonType comparisonType = null;

    public CompareNumbers(NumberComparisonType comparisonType) {
        super();
        this.comparisonType = comparisonType;
    }

    @Override
    public Stream transform(Stream stream) {
        com.jwplayer.sqe.trident.function.CompareNumbers function =
                new com.jwplayer.sqe.trident.function.CompareNumbers(comparisonType);

        return stream.each(getInputFields(), function, getOutputFields());
    }
}
