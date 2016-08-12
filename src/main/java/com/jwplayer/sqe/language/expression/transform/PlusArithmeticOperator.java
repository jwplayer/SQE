package com.jwplayer.sqe.language.expression.transform;


public class PlusArithmeticOperator extends ArithmeticOperator {
    public PlusArithmeticOperator() {
        super(ArithmeticOperatorType.Addition);
    }

    @Override
    public String getFunctionName() {
        return "+";
    }
}
