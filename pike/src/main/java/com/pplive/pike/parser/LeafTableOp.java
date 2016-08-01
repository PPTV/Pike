package com.pplive.pike.parser;

import com.pplive.pike.metadata.Table;

public final class LeafTableOp extends RelationalExprOperator {
	
	public LeafTableOp(Table table) {
		if (table == null)
			throw new IllegalArgumentException("table cannot be null");
		this._outputSchema = new RelationalExprSchema(table);
	}
	
	@Override
	public void setChild(RelationalExprOperator child){
		throw new UnsupportedOperationException("LeafTableOp cannot have any child");
	}
	@Override
	public int getChildCount(){
		return 0;
	}
	
	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toExplainString() {
		return String.format("Table %s%n", this._outputSchema.getName());
	}
}
