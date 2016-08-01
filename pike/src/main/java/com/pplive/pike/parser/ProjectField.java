package com.pplive.pike.parser;

import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.expression.ColumnExpression;

public final class ProjectField extends TransformField {
	
	public ProjectField(ColumnExpression expr) {
		this(expr, expr.getColumnName());
	}
	
	public ProjectField(ColumnExpression expr, String alias) {
		super(expr, alias);
	}
	
	public ProjectField(TransformField field) {
		super(field._expr, field._alias);
		if (this._expr.getClass() != ColumnExpression.class)
			throw new IllegalArgumentException("expression in field must be ColumnExpression");
	}

	public ColumnExpression getExpression() {
		return (ColumnExpression)this._expr;
	}
	
	@Override
	public boolean equals(Object o){
		if (o == this)
			return true;
		if (o == null || (o.getClass() != ProjectField.class))
			return false;
		ProjectField other = (ProjectField)o;
		return this._expr.equals(other._expr) && this._alias.equals(other._alias);
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}

