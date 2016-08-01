package com.pplive.pike.expression;

import java.io.Serializable;
import java.util.Map;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.metadata.Column;

import storm.trident.tuple.TridentTuple;

public abstract class AbstractExpression implements Serializable{

	private static final long serialVersionUID = 3025926321557869055L;

	public void init(){
	}
	
	public abstract Object eval(TridentTuple tuple);
	
	public void close(){
	}
	
	public abstract Class<?> exprType();
	
	public boolean needConvertResultToExprType() {
		// class BuiltinAggregatorExpression override this,
		// since builtin aggregate functions might actually return subclass of Number which hold necessary aggregate state. 
		return false;
	}
	
	public abstract Object visit(Object context, IExpressionVisitor visitor);
}
