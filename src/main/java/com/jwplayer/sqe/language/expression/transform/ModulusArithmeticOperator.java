package com.jwplayer.sqe.language.expression.transform;


public class ModulusArithmeticOperator extends ArithmeticOperator {
    public ModulusArithmeticOperator() {
        super(ArithmeticOperatorType.Modulus);
    }

    @Override
    public String getFunctionName() {
        return "%";
    }
}
