package com.pplive.pike.parser;

public abstract class RelationalExprOperator {

	protected RelationalExprOperator _parent;	
	public RelationalExprOperator getParent() {
		return this._parent;
	}
	public void setParent(RelationalExprOperator parent) {
		this._parent = parent;
	}

	protected RelationalExprOperator _child;
	public RelationalExprOperator getChild() {
		return this._child;
	}
	public void setChild(RelationalExprOperator child){
		this._child = child;
	}
	public int getChildCount(){
		return 1;
	}
	// return array, for consideration that CrossJoinOp/UnionOp may have more child nodes.
	public RelationalExprOperator[] getChildren(){
		if (this._child == null)
			return null;
		return new RelationalExprOperator[] { this._child };
	}
	
	protected RelationalExprSchema _outputSchema;
	public RelationalExprSchema getOutputSchema() {
		return this._outputSchema;
	}
	
	public String toExplainString() {
		return "(Relational Operation, child class need override toExplainString())\r\n";
	}
	
	public abstract Object accept(IRelationalOpVisitor visitor, Object context);
}
