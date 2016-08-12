package com.jwplayer.sqe.language.expression.transform;

import org.apache.storm.trident.Stream;


public class FormatDate extends TransformExpression {
    public FormatDate() {
    }

    @Override
    public String getFunctionName() {
        return "formatdate";
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new com.jwplayer.sqe.trident.function.FormatDate(), getOutputFields());
    }
}
