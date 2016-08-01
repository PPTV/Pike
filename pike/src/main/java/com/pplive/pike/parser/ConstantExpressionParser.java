package com.pplive.pike.parser;

import java.util.ArrayList;

import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ConstantExpression;


import net.sf.jsqlparser.expression.*;
//import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.statement.select.SubSelect;

class ConstantExpressionParser implements ExpressionVisitor {

	protected final Expression _expression;	
	protected AbstractExpression _parsedExpr;
	
	protected ArrayList<Exception> _parseErrors = new ArrayList<Exception>();
	protected void addError(Exception e){
		this._parseErrors.add(e);
	}

	public ConstantExpressionParser(Expression expression) {
		assert expression != null;
		this._expression = expression;
	}
	
	public AbstractExpression parse() {
		this._parsedExpr = null;
		this._expression.accept(this);
		if (this._parseErrors.size() > 0){
			throw new ParseErrorsException(this._parseErrors);
		}
		return this._parsedExpr;
	}
	
	// visit methods of ExpressionVisitor
	public void visit(NullValue nullValue) {
		this._parsedExpr = new ConstantExpression(null);
	}
	
	public void visit(Function function) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(SignedExpression signedExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
/*	public void visit(InverseExpression inverseExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}*/
	
	public void visit(JdbcParameter jdbcParameter) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(JdbcNamedParameter jdbcNamedParameter) {

	}

	public void visit(BooleanValue booleanValue) {
		boolean val = booleanValue.value();
		this._parsedExpr = new ConstantExpression(Boolean.valueOf(val));
	}
	
	public void visit(DoubleValue doubleValue) {
		double val = doubleValue.getValue();
		this._parsedExpr = new ConstantExpression(Double.valueOf(val));
	}
	
	public void visit(LongValue longValue) {
		long val = longValue.getValue();
		if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE)
			this._parsedExpr = new ConstantExpression(Integer.valueOf((int)val));
		else
			this._parsedExpr = new ConstantExpression(Long.valueOf(val));
	}

	public void visit(DateValue dateValue) {
		this._parsedExpr = new ConstantExpression(dateValue.getValue());
	}
	
	public void visit(TimeValue timeValue) {
		this._parsedExpr = new ConstantExpression(timeValue.getValue());
	}
	
	public void visit(TimestampValue timestampValue) {
		this._parsedExpr = new ConstantExpression(timestampValue.getValue());
	}
	
	public void visit(Parenthesis parenthesis) {
		Expression expr = parenthesis.getExpression();
		assert expr != null;
		expr.accept(this);
	}
	
	public void visit(StringValue stringValue) {
		this._parsedExpr = new ConstantExpression(stringValue.getValue());
	}
		
	public void visit(Addition addition) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(Division division) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(Multiplication multiplication) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(Subtraction subtraction) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(AndExpression andExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(OrExpression orExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(Between between) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(EqualsTo equalsTo) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(GreaterThan greaterThan) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(GreaterThanEquals greaterThanEquals) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(InExpression inExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(IsNullExpression isNullExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(LikeExpression likeExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(MinorThan minorThan) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(MinorThanEquals minorThanEquals) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(NotEqualsTo notEqualsTo) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(net.sf.jsqlparser.schema.Column tableColumn) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(SubSelect subSelect) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
		
	public void visit(CaseExpression caseExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(WhenClause whenClause) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(ExistsExpression existsExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(AllComparisonExpression allComparisonExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(Concat concat) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(Matches matches) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(BitwiseAnd bitwiseAnd) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(BitwiseOr bitwiseOr) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
	
	public void visit(BitwiseXor bitwiseXor) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(CastExpression castExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(Modulo modulo) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(AnalyticExpression analyticExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(ExtractExpression extractExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(IntervalExpression intervalExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}

	@Override
	public void visit(RegExpMatchOperator regExpMatchOperator) {
		addError(new IllegalStateException("should never happen"));
		this._parsedExpr = null;
	}
}
