package com.pplive.pike.expression;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.parser.SemanticErrorException;
import com.pplive.pike.util.ReflectionUtils;

import storm.trident.tuple.TridentTuple;

public final class BinaryOpExpression extends BuiltinOpExpression {

	private static final long serialVersionUID = 8708066777012604564L;

	private AbstractExpression _left;
	public AbstractExpression left() { return this._left; }
    public void setLeft(AbstractExpression expr) { this._left = expr; }

	private AbstractExpression _right;
	public AbstractExpression right() { return this._right; }
    public void setRight(AbstractExpression expr) { this._right = expr; }

	public BinaryOpExpression(String op, Class<?> opType, AbstractExpression left, AbstractExpression right) {
		super(op, opType);
		if (left == null || right == null)
			throw new IllegalArgumentException("left and right cannot be null");
		
		this._left = left;
		this._right = right;

		Class<?> leftType = this._left.exprType();
		Class<?> rightType = this._right.exprType();
		this._method = ReflectionUtils.tryGetMethod("eval", Arrays.asList(new Class<?>[]{leftType, rightType}), this._opType);
		if (this._method == null){
			throw new SemanticErrorException(String.format("<%s> %s <%s> is not supported", left.exprType().getSimpleName(), op, right.exprType().getSimpleName()));
		}
	}

	@Override
	public void init() {
		this._left.init();
		this._right.init();
		checkInitialized();
	}
	
	private void checkInitialized() {
		if (this._method != null)
			return;

		Class<?> leftType = this._left.exprType();
		Class<?> rightType = this._right.exprType();
		this._method = ReflectionUtils.getMethod("eval", Arrays.asList(new Class<?>[]{leftType, rightType}), this._opType);
		assert this._method != null;
	}

	@Override
	public Object eval(TridentTuple tuple) {
		Object left = this._left.eval(tuple);
		Object right = this._right.eval(tuple);

		checkInitialized(); // trident reducer aggregator has no prepare(), so expression call init() won't be called
                            // if it's passed as parameter in aggregate function call

		try {
			return this._method.invoke(null, left, right);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(String.format("calling builtin op class %s failed.", this._opType), e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(String.format("calling builtin op class %s failed.", this._opType), e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(String.format("calling builtin op class %s failed.", this._opType), e);
		}
	}
	
	@Override
	public Object visit(Object context, IExpressionVisitor visitor) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toString(){
		return String.format("(%s %s %s)", this._left, this._op, this._right);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || o.getClass() != BinaryOpExpression.class)
			return false;
		BinaryOpExpression other = (BinaryOpExpression)o;

		return (this._op.equals(other._op)
				&& this._opType == other._opType
				&& this._left.equals(other._left)
				&& this._right.equals(other._right));
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
