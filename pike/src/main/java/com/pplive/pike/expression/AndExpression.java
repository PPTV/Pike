package com.pplive.pike.expression;

import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.function.builtin.ConvertToBoolean;

import storm.trident.tuple.TridentTuple;

public final class AndExpression extends BinaryLogicalExpression {

	private static final long serialVersionUID = -4281572539593524147L;

	public AndExpression(AbstractExpression left, AbstractExpression right) {
		super(left, right);
	}

	@Override
	public Object eval(TridentTuple tuple) {
		Object left = this._left.eval(tuple);
		if (ConvertToBoolean.convert(left) == false) {
			return Boolean.FALSE;
		}
		
		Object right = this._right.eval(tuple);
		return ConvertToBoolean.convert(right);
	}
	
	@Override
	public Object visit(Object context, IExpressionVisitor visitor) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toString(){
		return String.format("(%s AND %s)", this._left, this._right);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || o.getClass() != AndExpression.class)
			return false;
		AndExpression other = (AndExpression)o;

		return (this._left.equals(other._left) && this._right.equals(other._right));
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
