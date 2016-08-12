package com.jwplayer.sqe.language.expression.transform;

import org.apache.storm.trident.Stream;


public class GetMapValue extends TransformExpression{
    public GetMapValue() {
    }

    @Override
    public String getFunctionName() {
        return "getmapvalue";
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new com.jwplayer.sqe.trident.function.GetMapValue(), getOutputFields());
    }
}
