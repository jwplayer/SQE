package com.jwplayer.sqe.language.expression.transform;

import org.apache.storm.trident.Stream;


public class ExpandValues extends TransformExpression {
    public ExpandValues() {
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new com.jwplayer.sqe.trident.function.ExpandValues(), getOutputFields());
    }

    @Override
    public String getFunctionName() {
        return "expandvalues";
    }
}
