package com.pplive.pike.expression;

public final class ExpressionAggregateCallInspector implements IExpressionVisitor {

    public static boolean containsAggregateCall(AbstractExpression expr) {
        return new ExpressionAggregateCallInspector().visit(null, expr);
    }

	private ExpressionAggregateCallInspector(){}
	
	private Boolean visit(Object context, AbstractExpression expr) {
		return (Boolean)expr.visit(null, this);
	}

	public Boolean visit(Object context, AggregateExpression expr) {
		return true;
	}

	public Boolean visit(Object context, BinaryOpExpression expr) {
		return visit(context, expr.left()) || visit(context, expr.right());
	}

	public Boolean visit(Object context, UnaryOpExpression expr) {
		return visit(context, expr.getParam());
	}

	private Boolean visit(Object context, BinaryLogicalExpression expr) {
		return visit(context, expr.left()) || visit(context, expr.right());
	}
	
	public Boolean visit(Object context, AndExpression expr) {
		return visit(context, (BinaryLogicalExpression)expr);
	}

    public Boolean visit(Object context, OrExpression expr) {
        return visit(context, (BinaryLogicalExpression)expr);
    }

    public Boolean visit(Object context, InExpression expr) {
        if (visit(context, expr.left()))
            return Boolean.TRUE;
        for(AbstractExpression candidate : expr.getCandidateExprs()){
            if (visit(context, candidate))
                return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

	public Boolean visit(Object context, ColumnExpression expr) {
		return false;
	}

	public Boolean visit(Object context, ConstantExpression expr) {
		return false;
	}

	public Boolean visit(Object context, FunctionExpression expr) {
		for(AbstractExpression paramExpr : expr.getParams()){
			if (visit(context, paramExpr))
				return true;
		}
		return false;
	}

	public Boolean visit(Object context, IfExpression expr) {
		if(visit(context, expr.condition()))
			return true;
		if(visit(context, expr.trueResult()))
			return true;
		if(visit(context, expr.falseResult()))
			return true;
		return false;
	}

	public Boolean visit(Object context, OutputExpression expr) {
		return false;
	}
}
