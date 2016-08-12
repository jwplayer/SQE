package com.jwplayer.sqe.language.expression.transform;

import com.jwplayer.sqe.trident.function.ProcessArithmeticOperator;
import org.apache.storm.trident.Stream;


public abstract class ArithmeticOperator extends TransformExpression {
    private ArithmeticOperatorType operatorType = null;

    public ArithmeticOperator(ArithmeticOperatorType operatorType) {
        super();
        this.operatorType = operatorType;
    }

    @Override
    public Stream transform(Stream stream) {
        return stream.each(getInputFields(), new ProcessArithmeticOperator(operatorType), getOutputFields());
    }
}
