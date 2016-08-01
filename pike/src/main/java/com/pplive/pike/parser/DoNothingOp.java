package com.pplive.pike.parser;

public class DoNothingOp extends RelationalExprOperator {

	public DoNothingOp(RelationalExprOperator child) {
		if (child == null)
			throw new IllegalArgumentException("child");
		
		this._child = child;
		this._outputSchema = child.getOutputSchema();
		child.setParent(this);
	}
	
	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}
		
	@Override
	public String toExplainString() {
		return "(DoNothingOp)\r\n";
	}
}
