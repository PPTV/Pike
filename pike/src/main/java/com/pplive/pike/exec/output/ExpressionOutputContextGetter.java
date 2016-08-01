package com.pplive.pike.exec.output;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.expression.*;
import com.pplive.pike.metadata.Column;

import java.util.ArrayList;
import java.util.List;

public final class ExpressionOutputContextGetter implements IExpressionVisitor {

	public static List<ConstantExpression> getAllOutputContextRefs(AbstractExpression expr) {
		return new ExpressionOutputContextGetter().visit(null, expr);
	}
	
	private ExpressionOutputContextGetter(){}
	
	@SuppressWarnings("unchecked")
	private List<ConstantExpression> visit(Object context, AbstractExpression expr) {
		return (List<ConstantExpression>)expr.visit(context, this);
	}

	public List<ConstantExpression> visit(Object context, AggregateExpression expr) {
		ArrayList<ConstantExpression> result = new ArrayList<ConstantExpression>(20);
		for(AbstractExpression paramExpr : expr.getParams()){
			List<ConstantExpression> exprs = visit(context, paramExpr);
			assert exprs != null;
			result.addAll(exprs);
		}
		return result;
	}

	public List<ConstantExpression> visit(Object context, BinaryOpExpression expr) {
		ArrayList<ConstantExpression> result = new ArrayList<ConstantExpression>(20);
		result.addAll(visit(context, expr.left()));
		result.addAll(visit(context, expr.right()));
		return result;
	}

	public List<ConstantExpression> visit(Object context, UnaryOpExpression expr) {
		return visit(context, expr.getParam());
	}

	private List<ConstantExpression> visit(Object context, BinaryLogicalExpression expr) {
		ArrayList<ConstantExpression> result = new ArrayList<ConstantExpression>(20);
		result.addAll(visit(context, expr.left()));
		result.addAll(visit(context, expr.right()));
		return result;
	}

	public List<ConstantExpression> visit(Object context, AndExpression expr) {
		return visit(context, (BinaryLogicalExpression)expr);
	}

    public List<ConstantExpression> visit(Object context, OrExpression expr) {
        return visit(context, (BinaryLogicalExpression)expr);
    }

    public List<ConstantExpression> visit(Object context, InExpression expr) {
        ArrayList<ConstantExpression> result = new ArrayList<ConstantExpression>(20);
        result.addAll(visit(context, expr.left()));
        for(AbstractExpression candidate : expr.getCandidateExprs())
            result.addAll(visit(context, candidate));
        return result;
    }

	public List<ConstantExpression> visit(Object context, ColumnExpression expr) {
        return new ArrayList<ConstantExpression>(0);
	}

	public List<ConstantExpression> visit(Object context, ConstantExpression expr) {
        if (expr.exprType() == OutputContext.class) {
            ArrayList<ConstantExpression> result = new ArrayList<ConstantExpression>(1);
            result.add(expr);
            return result;
        }
		return new ArrayList<ConstantExpression>(0);
	}

	public List<ConstantExpression> visit(Object context, FunctionExpression expr) {
		ArrayList<ConstantExpression> result = new ArrayList<ConstantExpression>(20);
		for(AbstractExpression paramExpr : expr.getParams()){
			List<ConstantExpression> exprs = visit(context, paramExpr);
			assert exprs != null;
			result.addAll(exprs);
		}
		return result;
	}

	public List<ConstantExpression> visit(Object context, IfExpression expr) {
		ArrayList<ConstantExpression> result = new ArrayList<ConstantExpression>(20);
		result.addAll(visit(context, expr.condition()));
		result.addAll(visit(context, expr.trueResult()));
		result.addAll(visit(context, expr.falseResult()));
		return result;
	}

	public List<ConstantExpression> visit(Object context, OutputExpression expr) {
		return visit(context, expr.getExpr());
	}
}
