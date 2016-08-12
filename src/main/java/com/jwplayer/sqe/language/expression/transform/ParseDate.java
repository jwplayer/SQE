package com.jwplayer.sqe.language.expression.transform;

import org.apache.storm.trident.Stream;


public class ParseDate extends TransformExpression {
    public ParseDate() {
    }

    @Override
    public String getFunctionName() {
        return "parsedate";
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new com.jwplayer.sqe.trident.function.ParseDate(), getOutputFields());
    }
}
