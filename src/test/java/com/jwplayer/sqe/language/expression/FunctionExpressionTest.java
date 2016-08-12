package com.jwplayer.sqe.language.expression;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.GsonBuilder;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FunctionExpressionTest {
    GsonBuilder gsonBuilder = null;

    @Before
    public void setup() {
        gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(BaseExpression.class,
                new BaseExpression.BaseExpressionTypeAdapter());
    }

    @After
    public void shutdown() {

    }

    @Test
    public void testMakeFunction() {

        List<BaseExpression> arguments = new ArrayList<>();

        BaseExpression integerConstant1 = gsonBuilder.create().fromJson("{\"C\":0}", BaseExpression.class);
        arguments.add(integerConstant1);
        BaseExpression integerConstant2 = gsonBuilder.create().fromJson("{\"C\":1}", BaseExpression.class);
        arguments.add(integerConstant2);
        FunctionExpression test = FunctionExpression.makeFunction("+", arguments);
        assertEquals(test.getClass().toString(),
                "class com.jwplayer.sqe.language.expression.transform.PlusArithmeticOperator");

        assertEquals(test.getFunctionName(), "+");

    }

}