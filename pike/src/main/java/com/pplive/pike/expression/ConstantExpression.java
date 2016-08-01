package com.pplive.pike.expression;

import java.util.HashMap;
import java.util.Map;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.metadata.Column;

import storm.trident.tuple.TridentTuple;

public final class ConstantExpression extends AbstractExpression {
	
	private static final long serialVersionUID = -2308666499356311528L;
	private Object _value;
	private final Class<?> _nullType;
	
	public static ConstantExpression NullValue(Class<?> nullType){
		return new ConstantExpression(nullType);
	}
	
	private ConstantExpression(Class<?> nullType){
		this._value = null;
		this._nullType = nullType;
	}
	
	public ConstantExpression(Object obj){
		this._value = obj;
		this._nullType = Object.class;
	}

    public void setValue(Object val) {
        this._value = val;
    }
	
	@Override
	public Object eval(TridentTuple tuple) {
		return _value;
	}
	
	@Override
	public Object visit(Object context, IExpressionVisitor visitor) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toString(){
		if (this._value == null)
            return "<NULL>";
        Class<?> t = this.exprType();
        if (t == Character.class) {
            return String.format("'%s'", this._value);
        }
        if (t == String.class) {
            return String.format("\"%s\"", this._value);
        }
        return this._value.toString();
	}
	
	@Override
	public Class<?> exprType() {
		if (this._value == null)
			return this._nullType;
		return this._value.getClass();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || o.getClass() != ConstantExpression.class)
			return false;
		ConstantExpression other = (ConstantExpression)o;
		if (this._value == null){
			return other._value == null;
		}
		return this._value.equals(other._value);
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
