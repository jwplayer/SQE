package com.jwplayer.sqe.language.expression;

import java.util.*;

import org.apache.storm.tuple.Fields;


public abstract class FunctionExpression extends BaseExpression implements Cloneable {
    protected static ServiceLoader<FunctionExpression> loader = null;
    protected List<BaseExpression> arguments;

    public FunctionExpression() {
    }

    @Override
    public boolean equals(Object expression) {
        if (expression.getClass() != getClass())
            return false;
        FunctionExpression exp = (FunctionExpression) expression;
        if (exp.getArguments().size() != getArguments().size())
            return false;
        boolean isEqual;

        isEqual = getFunctionName().equals(exp.getFunctionName());

        for (int i = 0; i < getArguments().size(); i++) {
            isEqual = isEqual && getArguments().get(i).equals(exp.getArguments().get(i));
        }

        return isEqual;
    }

    public List<BaseExpression> getArguments() {
        return arguments;
    }

    @Override
    public ExpressionType getExpressionType() {
        return ExpressionType.Function;
    }

    public abstract String getFunctionName();

    public abstract FunctionType getFunctionType();

    public Fields getInputFields() {
        List<String> inputFields = new ArrayList<>();

        for (BaseExpression argument : getArguments())
            inputFields.add(argument.getOutputFieldName());

        return new Fields(inputFields);
    }

    @Override
    public String getOutputFieldName() {
        StringBuilder sb = new StringBuilder();
        sb.append(getFunctionName()).append("(");

        for (int i = 0; i < getArguments().size(); i++) {
            if (i > 0)
                sb.append(",");
            sb.append(getArguments().get(i).getOutputFieldName());
        }

        sb.append(")");

        return sb.toString();
    }

    public Fields getOutputFields() {
        return new Fields(getOutputFieldName());
    }

    public void setArguments(List<BaseExpression> arguments) {
        this.arguments = arguments;
    }

    public List<BaseExpression> unRoll() {
        List<BaseExpression> retVal = new ArrayList<>();

        for (int i = 0; i < getArguments().size(); i++) {
            List<BaseExpression> expressionSet = getArguments().get(i).unRoll();

            if (expressionSet != null) {
                expressionSet.add(getArguments().get(i));
                getArguments().set(i, new FieldExpression(getArguments().get(i)
                        .getOutputFieldName()));
                retVal.addAll(expressionSet);
            }
        }

        return retVal;
    }

    public static FunctionExpression makeFunction(String functionName, List<BaseExpression> arguments) {
        try {
            if (loader == null) {
                loader = ServiceLoader.load(FunctionExpression.class);
            }

            FunctionExpression retVal;

            for (FunctionExpression expression : loader) {
                if (expression.getFunctionName().equalsIgnoreCase(functionName)) {
                    retVal = (FunctionExpression) expression.clone();
                    retVal.setArguments(arguments);

                    return retVal;
                }
            }
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }

        throw new RuntimeException(functionName + " is not a valid function expression");
    }
}
