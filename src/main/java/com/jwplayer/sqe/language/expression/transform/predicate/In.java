package com.jwplayer.sqe.language.expression.transform.predicate;

import com.jwplayer.sqe.trident.function.IsIn;
import org.apache.storm.trident.Stream;


public class In extends PredicateExpression {
    public In() {
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new IsIn(), getOutputFields());
    }

    @Override
    public String getFunctionName() {
        return "in";
    }
}
