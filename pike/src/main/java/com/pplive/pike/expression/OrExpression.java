package com.pplive.pike.expression;

import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.function.builtin.ConvertToBoolean;

import storm.trident.tuple.TridentTuple;

public final class OrExpression extends BinaryLogicalExpression {

	private static final long serialVersionUID = -6151794826376300817L;

	public OrExpression(AbstractExpression left, AbstractExpression right ) {
		super(left, right);
	}

	@Override
	public Object eval(TridentTuple tuple) {
		Object left = this._left.eval(tuple);
		if (ConvertToBoolean.convert(left)) {
			return Boolean.TRUE;
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
		return String.format("(%s OR %s)", this._left, this._right);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || o.getClass() != OrExpression.class)
			return false;
		OrExpression other = (OrExpression)o;

		return (this._left.equals(other._left) && this._right.equals(other._right));
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
