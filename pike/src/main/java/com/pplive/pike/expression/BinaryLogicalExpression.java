package com.pplive.pike.expression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.metadata.Column;

public abstract class BinaryLogicalExpression extends AbstractExpression {

	private static final long serialVersionUID = -6502684990365153515L;

	protected AbstractExpression _left;
	public AbstractExpression left() { return this._left; }
    public void setLeft(AbstractExpression expr) { this._left = expr; }

	protected AbstractExpression _right;
	public AbstractExpression right() { return this._right; }
    public void setRight(AbstractExpression expr) { this._right = expr; }

	public BinaryLogicalExpression(AbstractExpression left, AbstractExpression right ) {
		if (left == null)
			throw new IllegalArgumentException("left cannot be null");
		if (right == null)
			throw new IllegalArgumentException("right cannot be null");
		
		this._left = left;
		this._right = right;
	}

	@Override
	public void init() {
		this._left.init();
		this._right.init();
	}
	
	@Override
	public Class<?> exprType() {
		return Boolean.class;
	}
}
