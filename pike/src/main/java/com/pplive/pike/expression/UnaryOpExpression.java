package com.pplive.pike.expression;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import storm.trident.tuple.TridentTuple;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.parser.SemanticErrorException;
import com.pplive.pike.util.ReflectionUtils;


public final class UnaryOpExpression extends BuiltinOpExpression {

	private static final long serialVersionUID = -4114001332192623231L;

	private AbstractExpression _param;
    public AbstractExpression getParam() { return this._param; }
    public void setParam(AbstractExpression expr) { this._param = expr; }

	public UnaryOpExpression(String op, Class<?> opType, AbstractExpression param) {
		super(op, opType);
		if (param == null)
			throw new IllegalArgumentException("param cannot be null");
		
		this._param = param;

		Class<?> paramType = this._param.exprType();
		this._method = ReflectionUtils.tryGetMethod("eval", Arrays.asList(new Class<?>[]{paramType}), this._opType);
		if (this._method == null){
			throw new SemanticErrorException(String.format("%s <%s> is not supported", op, param.exprType().getSimpleName()));
		}
	}

	@Override
	public void init() {
		this._param.init();
		Class<?> paramType = this._param.exprType();
		this._method = ReflectionUtils.getMethod("eval", Arrays.asList(new Class<?>[]{paramType}), this._opType);
	}
	
	private void checkInitialized() {
		if (this._method != null)
			return;

		Class<?> paramType = this._param.exprType();
		this._method = ReflectionUtils.getMethod("eval", Arrays.asList(new Class<?>[]{paramType}), this._opType);
		assert this._method != null;
	}

	@Override
	public Object eval(TridentTuple tuple) {
		Object paramValue = this._param.eval(tuple);

		checkInitialized();
		try {
			return this._method.invoke(null, paramValue);
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
		return String.format("(%s %s)", this._op, this._param);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || o.getClass() != UnaryOpExpression.class)
			return false;
		UnaryOpExpression other = (UnaryOpExpression)o;

		return (this._op.equals(other._op)
				&& this._opType == other._opType
				&& this._param.equals(other._param));
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
