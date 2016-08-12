package com.jwplayer.sqe.language.expression.transform.predicate;

import com.jwplayer.sqe.trident.function.CompareObjects;
import org.apache.storm.trident.Stream;


public class Equals extends PredicateExpression {
    public Equals() {
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new CompareObjects(), getOutputFields());
    }

    @Override
    public String getFunctionName() {
        return "=";
    }
}
