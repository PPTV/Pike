package com.pplive.pike.expression;

public interface IExpressionVisitor {

	public Object visit(Object context, AggregateExpression expr);
	public Object visit(Object context,BinaryOpExpression expr);
	public Object visit(Object context,UnaryOpExpression expr);
	public Object visit(Object context,AndExpression expr);
    public Object visit(Object context,OrExpression expr);
    public Object visit(Object context,InExpression expr);
	public Object visit(Object context,ColumnExpression expr);
	public Object visit(Object context,ConstantExpression expr);
	public Object visit(Object context,FunctionExpression expr);
	public Object visit(Object context,IfExpression expr);
	public Object visit(Object context,OutputExpression expr);
}
