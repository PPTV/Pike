package com.pplive.pike.expression;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.util.ReflectionUtils;

import storm.trident.tuple.TridentTuple;

public final class OutputExpression extends AbstractExpression {
	
	private static final long serialVersionUID = 1L;
	private final AbstractExpression _expr;
    public AbstractExpression getExpr() {
        return this._expr;
    }
	
	public OutputExpression(AbstractExpression expr){
		if (expr == null)
			throw new IllegalArgumentException("expr cannot be null");
		if (checkExpressionRequirement(expr) == false){
			assert false;
			throw new IllegalArgumentException("expr cannot reference any column, variable or aggregate function call");
		}
		this._expr = expr;
	}
	
	private static boolean checkExpressionRequirement(AbstractExpression expr){
		return ExpressionColumnRefInspector.containsColumnOrVariableReference(expr) == false
				&& ExpressionAggregateCallInspector.containsAggregateCall(expr) == false;
	}

	@Override
	public void init() {
		this._expr.init();
	}
	
	@Override
	public Object eval(TridentTuple tuple) {
		return this._expr.eval(tuple);
	}
	
	@Override
	public Object visit(Object context, IExpressionVisitor visitor) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toString(){
		return String.format("OUTPUT(%s)", this._expr);
	}
	
	@Override
	public Class<?> exprType() {
		return this._expr.exprType();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || o.getClass() != OutputExpression.class)
			return false;
		OutputExpression other = (OutputExpression)o;
		return this._expr.equals(other._expr);
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
