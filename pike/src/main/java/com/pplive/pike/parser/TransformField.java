package com.pplive.pike.parser;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ColumnExpression;

public class TransformField {
	protected final AbstractExpression _expr;
	public AbstractExpression getExpression() {
		return this._expr;
	}
	
	protected final CaseIgnoredString _alias;
	public CaseIgnoredString getAlias() {
		return this._alias;
	}
	
	public TransformField(AbstractExpression expr) {
		this(expr, "");
	}
	
	public TransformField(AbstractExpression expr, String alias) {
		this(expr, new CaseIgnoredString(alias));
	}
	
	public TransformField(AbstractExpression expr, CaseIgnoredString alias) {
		assert expr != null;
		assert alias != null;
		this._expr = expr;
		if (alias.isEmpty() == false) {
			this._alias = alias;
		}
		else if (expr instanceof ColumnExpression) {
			String col = ((ColumnExpression)expr).getColumnName();
			assert col != null && col.isEmpty() == false;
			this._alias = new CaseIgnoredString(col);
		}
		else{
			this._alias = InternalColumnName.genColumnName();
		}
	}
	
	@Override
	public boolean equals(Object o){
		if (o == this)
			return true;
		if (o == null || (o.getClass() != this.getClass()))
			return false;
		TransformField other = (TransformField)o;
		return this._expr.equals(other._expr) && this._alias.equals(other._alias);
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}

