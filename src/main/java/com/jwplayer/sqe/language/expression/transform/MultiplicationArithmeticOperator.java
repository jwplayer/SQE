package com.jwplayer.sqe.language.expression.transform;


public class MultiplicationArithmeticOperator extends ArithmeticOperator {
    public MultiplicationArithmeticOperator() {
        super(ArithmeticOperatorType.Multiplication);
    }

    @Override
    public String getFunctionName() {
        return "*";
    }
}
