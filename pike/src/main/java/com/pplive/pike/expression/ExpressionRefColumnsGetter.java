package com.pplive.pike.expression;

import java.util.HashMap;
import java.util.Map;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.metadata.Column;

public final class ExpressionRefColumnsGetter implements IExpressionVisitor {

	public static Map<CaseIgnoredString, Column> getAllReferencedColumns(AbstractExpression expr) {
		return new ExpressionRefColumnsGetter().visit(null, expr);
	}
	
	private ExpressionRefColumnsGetter(){}
	
	@SuppressWarnings("unchecked")
	private Map<CaseIgnoredString, Column> visit(Object context, AbstractExpression expr) {
		return (Map<CaseIgnoredString, Column>)expr.visit(null, this);
	}

	public Map<CaseIgnoredString, Column> visit(Object context, AggregateExpression expr) {
		HashMap<CaseIgnoredString, Column> result = new HashMap<CaseIgnoredString, Column>(20);
		for(AbstractExpression paramExpr : expr.getParams()){
			Map<CaseIgnoredString, Column> cols = visit(context, paramExpr);
			assert cols != null;
			result.putAll(cols);
		}
		return result;
	}

	public Map<CaseIgnoredString, Column> visit(Object context, BinaryOpExpression expr) {
		HashMap<CaseIgnoredString, Column> result = new HashMap<CaseIgnoredString, Column>(20);
		result.putAll(visit(context, expr.left()));
		result.putAll(visit(context, expr.right()));
		return result;
	}

	public Map<CaseIgnoredString, Column> visit(Object context, UnaryOpExpression expr) {
		return visit(context, expr.getParam());
	}

	private Map<CaseIgnoredString, Column> visit(Object context, BinaryLogicalExpression expr) {
		HashMap<CaseIgnoredString, Column> result = new HashMap<CaseIgnoredString, Column>(20);
		result.putAll(visit(context, expr.left()));
		result.putAll(visit(context, expr.right()));
		return result;
	}

	public Map<CaseIgnoredString, Column> visit(Object context, AndExpression expr) {
		return visit(context, (BinaryLogicalExpression)expr);
	}

	public Map<CaseIgnoredString, Column> visit(Object context, OrExpression expr) {
		return visit(context, (BinaryLogicalExpression)expr);
	}

    public Map<CaseIgnoredString, Column> visit(Object context, InExpression expr) {
        HashMap<CaseIgnoredString, Column> result = new HashMap<CaseIgnoredString, Column>(20);
        result.putAll(visit(context, expr.left()));
        for(AbstractExpression candidate : expr.getCandidateExprs())
            result.putAll(visit(context, candidate));
        return result;
    }

	public Map<CaseIgnoredString, Column> visit(Object context, ColumnExpression expr) {
		HashMap<CaseIgnoredString, Column> result = new HashMap<CaseIgnoredString, Column>(1);
		result.put(new CaseIgnoredString(expr.getColumnName()), new Column(expr.getColumnName(), "", expr.getColumnType()));
		return result;
	}

	public Map<CaseIgnoredString, Column> visit(Object context, ConstantExpression expr) {
		return new HashMap<CaseIgnoredString, Column>(0);
	}

	public Map<CaseIgnoredString, Column> visit(Object context, FunctionExpression expr) {
		HashMap<CaseIgnoredString, Column> result = new HashMap<CaseIgnoredString, Column>(20);
		for(AbstractExpression paramExpr : expr.getParams()){
			Map<CaseIgnoredString, Column> cols = visit(context, paramExpr);
			assert cols != null;
			result.putAll(cols);
		}
		return result;
	}

	public Map<CaseIgnoredString, Column> visit(Object context, IfExpression expr) {
		HashMap<CaseIgnoredString, Column> result = new HashMap<CaseIgnoredString, Column>(20);
		result.putAll(visit(context, expr.condition()));
		result.putAll(visit(context, expr.trueResult()));
		result.putAll(visit(context, expr.falseResult()));
		return result;
	}

	public Map<CaseIgnoredString, Column> visit(Object context, OutputExpression expr) {
		return new HashMap<CaseIgnoredString, Column>(0);
	}
}
