package com.pplive.pike.parser;

import com.pplive.pike.expression.AbstractExpression;

import com.pplive.pike.expression.ExpressionAggregateCallInspector;
import net.sf.jsqlparser.expression.Expression;

class WhereConditionParser {

	private final PikeSqlParser _sqlParser;
	private final RelationalExprOperator _childOp;
	private final SchemaScope _schemaScope;
	private final Expression _expression;
	private final boolean _having;
	
	public WhereConditionParser(PikeSqlParser sqlParser, RelationalExprOperator childOp, SchemaScope schemaScope, Expression expression) {
		this(sqlParser, childOp, schemaScope, expression, false);
	}
	
	public WhereConditionParser(PikeSqlParser sqlParser, RelationalExprOperator childOp, SchemaScope schemaScope, Expression expression, boolean having) {
		assert sqlParser != null;
		assert childOp != null;
		assert expression != null;
		
		this._sqlParser = sqlParser;
		this._childOp = childOp;
		this._schemaScope = schemaScope != null ? schemaScope : new SchemaScope(childOp.getOutputSchema());
		this._expression = expression;
		this._having = having;
	}
	
	public RelationalExprOperator parse() {
		Expression expr = this._expression;
		ExpressionParser parser = new ExpressionParser(this._sqlParser, this._schemaScope, expr);
		AbstractExpression parsedExpr = parser.parse();
		if (parsedExpr.exprType() != Boolean.class
				&& parsedExpr.exprType() != boolean.class) {
			String msg = String.format("expression in %s clause must be boolean type", this._having ? "HAVING" : "WHERE");
			throw new ParseErrorsException(new SemanticErrorException(msg));
		}
        if (ExpressionAggregateCallInspector.containsAggregateCall(parsedExpr)) {
            String msg = "Where/Having ... clause cannot contain aggregation function call.";
            throw new ParseErrorsException(new SemanticErrorException(msg));
        }
		return new SelectOp(this._childOp, parsedExpr, this._having);
	}
	
}
