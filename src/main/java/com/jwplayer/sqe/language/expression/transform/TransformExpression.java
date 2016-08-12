package com.jwplayer.sqe.language.expression.transform;

import com.jwplayer.sqe.language.expression.FunctionExpression;
import com.jwplayer.sqe.language.expression.FunctionType;
import org.apache.storm.trident.Stream;


public abstract class TransformExpression extends FunctionExpression {
    public TransformExpression() {
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.Transform;
    }

    public abstract Stream transform(Stream stream);
}
