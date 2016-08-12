package com.jwplayer.sqe.language.expression;

import java.util.List;

public class FieldExpression extends BaseExpression {
    public String fieldName;

    public FieldExpression(String fieldName) { this.fieldName = fieldName; }

    @Override
    public boolean equals(Object expression) {
        return (expression.getClass() == this.getClass()
                && this.fieldName.equals(((FieldExpression) expression).fieldName));
    }

    @Override
    public ExpressionType getExpressionType() {
        return ExpressionType.Field;
    }

    @Override
    public String getOutputFieldName() {
        return fieldName;
    }

    @Override
    public List<BaseExpression> unRoll() {
        return null;
    }
}
