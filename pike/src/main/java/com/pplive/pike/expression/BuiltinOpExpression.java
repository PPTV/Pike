package com.pplive.pike.expression;

import java.lang.reflect.Method;


public abstract class BuiltinOpExpression extends AbstractExpression {

	private static final long serialVersionUID = 5282347587007287072L;

	protected String _op;
	protected Class<?> _opType;
	
	protected transient Method _method;

	protected BuiltinOpExpression(String op, Class<?> opType) {
		if (op == null || op.isEmpty())
			throw new IllegalArgumentException("op cannot be null or empty");
		if (opType == null)
			throw new IllegalArgumentException("opType cannot be null");
		
		this._op = op;
		this._opType = opType;
	}
	
	@Override
	public Class<?> exprType() {
		assert this._method != null;
		return this._method.getReturnType();
	}
}
