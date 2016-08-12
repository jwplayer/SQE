package com.jwplayer.sqe.language.expression.transform;

import com.jwplayer.sqe.trident.function.GetIf;
import org.apache.storm.trident.Stream;


public class If extends TransformExpression {
    public If() {
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new GetIf(), getOutputFields());
    }

    @Override
    public String getFunctionName() {
        return "if";
    }
}
