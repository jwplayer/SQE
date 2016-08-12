package com.jwplayer.sqe.language.expression.transform;


public class DivisionArithmeticOperator extends ArithmeticOperator {
    public DivisionArithmeticOperator() {
        super(ArithmeticOperatorType.Division);
    }

    @Override
    public String getFunctionName() {
        return "/";
    }
}
