package com.jwplayer.sqe.language.expression;

import java.util.ArrayList;
import java.util.List;

public class ConstantExpression extends BaseExpression {
    public Object constant;

    public ConstantExpression(Object constant) {
        this.constant = constant;
    }

    @Override
    public boolean equals(Object expression) {
        return (expression.getClass() == this.getClass()
                && this.constant.equals(((ConstantExpression) expression).constant));
    }

    @Override
    public ExpressionType getExpressionType() {
        return ExpressionType.Constant;
    }

    @Override
    public String getOutputFieldName() {
        return constant == null ?
                "_C_NULL" :
                "_C_" + constant.getClass().toString() + "_" + constant.toString();
    }

    @Override
    public List<BaseExpression> unRoll() {
        return new ArrayList<>();
    }
}
