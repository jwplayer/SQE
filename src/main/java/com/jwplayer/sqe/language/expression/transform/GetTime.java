package com.jwplayer.sqe.language.expression.transform;

import org.apache.storm.trident.Stream;


public class GetTime extends TransformExpression {
    public GetTime() {
    }

    @Override
    public String getFunctionName() {
        return "gettime";
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new com.jwplayer.sqe.trident.function.GetTime(), getOutputFields());
    }
}
