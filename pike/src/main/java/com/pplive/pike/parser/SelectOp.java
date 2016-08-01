package com.pplive.pike.parser;

import com.pplive.pike.expression.AbstractExpression;

public class SelectOp extends RelationalExprOperator {
	
	private AbstractExpression _condition;
	public AbstractExpression conditionExpr() {
		return this._condition;
	}
	
	private final boolean _having;
	public boolean isHaving() {
		return this._having;
	}

	public SelectOp(RelationalExprOperator child, AbstractExpression condition, boolean having) {
		if (child == null)
			throw new IllegalArgumentException("child cannot be null");
		if (condition == null)
			throw new IllegalArgumentException("condition cannot be null");
		
		this._condition = condition;
		this._having = having;
		this._outputSchema = child.getOutputSchema();
		child.setParent(this);
		this._child = child;
	}
	
	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toExplainString() {
		return this._child.toExplainString() 
				+ String.format("Filter(%s):%n",isHaving() ? "having" : "where")
				+ String.format("\ttable: %s%n", this._outputSchema.getName())
				+ String.format("\tcondition: %s%n", this._condition.toString());
	}
}
