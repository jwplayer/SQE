package com.jwplayer.sqe.language.expression.transform;

import org.apache.storm.trident.Stream;


public class RoundDate extends TransformExpression {
    public RoundDate() {
    }

    @Override
    public String getFunctionName() {
        return "rounddate";
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new com.jwplayer.sqe.trident.function.RoundDate(), getOutputFields());
    }
}
