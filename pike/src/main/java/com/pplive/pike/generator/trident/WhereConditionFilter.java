package com.pplive.pike.generator.trident;

import java.util.Map;

import com.pplive.pike.expression.AbstractExpression;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class WhereConditionFilter extends BaseFilter {

	private static final long serialVersionUID = 4988101855902545208L;
	private AbstractExpression _expr;

	public WhereConditionFilter(AbstractExpression expr) {
		this._expr = expr;
	}

	@Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		_expr.init();
    }

	@Override
	public boolean isKeep(TridentTuple tuple) {
		Boolean keep = (Boolean)this._expr.eval(tuple);
		return keep != null && keep;
	}

	 @Override
	 public void cleanup() {
	 }
}
