package com.jwplayer.sqe.language.expression.transform.predicate;

import com.jwplayer.sqe.trident.function.EvaluateRegularExpression;
import org.apache.storm.trident.Stream;


public class RLike extends PredicateExpression {
    public RLike() {
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new EvaluateRegularExpression(), getOutputFields());
    }

    @Override
    public String getFunctionName() {
        return "rlike";
    }
}
