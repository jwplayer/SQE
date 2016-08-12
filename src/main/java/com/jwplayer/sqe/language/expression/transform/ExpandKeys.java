package com.jwplayer.sqe.language.expression.transform;

import org.apache.storm.trident.Stream;


public class ExpandKeys extends TransformExpression {
    public ExpandKeys() {
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new com.jwplayer.sqe.trident.function.ExpandKeys(), getOutputFields());
    }

    @Override
    public String getFunctionName() {
        return "expandkeys";
    }
}
