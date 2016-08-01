package com.pplive.pike.expression;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.function.builtin.BuiltinFunctions;
import com.pplive.pike.function.builtin.Convert;
import com.pplive.pike.function.builtin.ConvertToBoolean;
import com.pplive.pike.metadata.Column;

import storm.trident.tuple.TridentTuple;

public final class IfExpression extends AbstractExpression {

	private static final long serialVersionUID = -4281572539593524147L;
	private AbstractExpression _condition;
    public AbstractExpression condition() { return this._condition; }
    public void setCondition(AbstractExpression expr) { this._condition = expr; }

	private AbstractExpression _trueResult;
	public AbstractExpression trueResult() { return this._trueResult; }
    public void setTrueResult(AbstractExpression expr) { this._trueResult = expr; }

	private AbstractExpression _falseResult;
	public AbstractExpression falseResult() { return this._falseResult; }
    public void setFalseResult(AbstractExpression expr) { this._falseResult = expr; }

	public IfExpression(AbstractExpression condition, AbstractExpression trueResult, AbstractExpression falseResult ) {
		if (condition == null)
			throw new IllegalArgumentException("condition cannot be null");
		if (trueResult == null)
			throw new IllegalArgumentException("trueResult cannot be null");
		if (falseResult == null)
			throw new IllegalArgumentException("falseResult cannot be null");
		
		this._condition = condition;
		int checkResult = checkTypeConversion(trueResult, falseResult);
		if (checkResult < 0){
			String targetType = falseResult.exprType().getSimpleName();
			trueResult = BuiltinFunctions.getFunctionParser(targetType).parse(targetType, Arrays.asList(trueResult));
		}
		else if (checkResult > 0){
			String targetType = trueResult.exprType().getSimpleName();
			falseResult = BuiltinFunctions.getFunctionParser(targetType).parse(targetType, Arrays.asList(falseResult));
		}

		if (trueResult.exprType() != falseResult.exprType()){
			assert false;
			throw new IllegalStateException("should never happen: if() result type is inconsistent on true result and false result");
		}
		this._trueResult = trueResult;
		this._falseResult = falseResult;
	}

	@Override
	public void init() {
		this._condition.init();
		this._trueResult.init();
		this._falseResult.init();
	}

	@Override
	public Object eval(TridentTuple tuple) {
		Object conditionVal = this._condition.eval(tuple);
		if (conditionVal != null && ConvertToBoolean.convert(conditionVal)) {
			return this._trueResult.eval(tuple);
		}
		return this._falseResult.eval(tuple);
	}
	
	@Override
	public Object visit(Object context, IExpressionVisitor visitor) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toString(){
		return String.format("if(%s, %s, %s)", this._condition, this._trueResult, this._falseResult);
	}
	
	@Override
	public Class<?> exprType() {
		// TODO
		return this._trueResult.exprType();
	}
	
	public static boolean isCompatibleType(AbstractExpression trueResult, AbstractExpression falseResult) {
		Class<?> left = trueResult.exprType();
		Class<?> right = falseResult.exprType();
		
		return left == right
				|| (Number.class.isAssignableFrom(left) && Number.class.isAssignableFrom(right));
	}
	
	private static int checkTypeConversion(AbstractExpression trueResult, AbstractExpression falseResult) {
		Class<?> left = trueResult.exprType();
		Class<?> right = falseResult.exprType();
		if (left == right)
			return 0;
		assert Number.class.isAssignableFrom(left) && Number.class.isAssignableFrom(right);
		if (Number.class.isAssignableFrom(left) == false || Number.class.isAssignableFrom(right) == false)
			throw new IllegalStateException("should never happen: passed expression type is not number");
		
		return getPrimitiveConvertOrder(trueResult.exprType()) > getPrimitiveConvertOrder(falseResult.exprType())
				? 1 : -1;
	}
	
	private static int getPrimitiveConvertOrder(Class<?> t) {
		if (t == Byte.class) return 1;
		if (t == Short.class) return 2;
		if (t == Integer.class) return 3;
		if (t == Long.class) return 4;
		if (t == Float.class) return 5;
		if (t == Double.class) return 6;
		throw new IllegalStateException("should never happen: passed type is not number");
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o == this)
			return true;
		if (o.getClass() != IfExpression.class)
			return false;
		IfExpression other = (IfExpression)o;

		return (this._condition.equals(other._condition)
				&& this._trueResult.equals(other._trueResult)
				&& this._falseResult.equals(other._falseResult) );
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
