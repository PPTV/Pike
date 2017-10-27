package com.pplive.pike.expression;

import storm.trident.tuple.TridentTuple;

import java.util.Random;

/**
 * Created by jiatingjin on 2017/10/24.
 * 用于unit test
 */
public class MockExpression extends AbstractExpression {
    private static Random random = new Random();
    @Override
    public Object eval(TridentTuple tuple) {
        return random.nextInt();
    }

    @Override
    public Class<?> exprType() {
        return null;
    }

    @Override
    public Object visit(Object context, IExpressionVisitor visitor) {
        return null;
    }
}
