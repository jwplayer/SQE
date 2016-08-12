package com.jwplayer.sqe.language.expression.transform;

import org.apache.storm.trident.Stream;


public class Hash extends TransformExpression {
    public Hash() {
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new com.jwplayer.sqe.trident.function.Hash(), getOutputFields());
    }

    @Override
    public String getFunctionName() {
        return "hash";
    }
}
