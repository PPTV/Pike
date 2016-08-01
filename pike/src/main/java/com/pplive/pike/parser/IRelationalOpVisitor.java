package com.pplive.pike.parser;

public interface IRelationalOpVisitor {

	public Object visit(Object context, DoNothingOp op);
	public Object visit(Object context, LeafTableOp op);
	public Object visit(Object context, RenameOp op);
	public Object visit(Object context, SelectOp op);
	public Object visit(Object context, ProjectOp op);
	public Object visit(Object context, TransformOp op);
	public Object visit(Object context, AggregateOp op);
    public Object visit(Object context, TopOp op);
    public Object visit(Object context, LateralOp op);

}
