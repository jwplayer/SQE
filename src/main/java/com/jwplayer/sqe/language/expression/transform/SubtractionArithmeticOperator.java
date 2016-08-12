package com.jwplayer.sqe.language.expression.transform;


public class SubtractionArithmeticOperator extends ArithmeticOperator {
    public SubtractionArithmeticOperator() {
        super(ArithmeticOperatorType.Subtraction);
    }

    @Override
    public String getFunctionName() {
        return "-";
    }
}
