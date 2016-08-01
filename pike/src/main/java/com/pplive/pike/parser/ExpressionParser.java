package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.base.SizeAwareIterable;
import com.pplive.pike.expression.*;
import com.pplive.pike.expression.arithmetic.AddOp;
import com.pplive.pike.expression.arithmetic.DivisionOp;
import com.pplive.pike.expression.arithmetic.MultiOp;
import com.pplive.pike.expression.arithmetic.SubtractOp;
import com.pplive.pike.expression.compare.EqualToOp;
import com.pplive.pike.expression.compare.GreaterThanOp;
import com.pplive.pike.expression.compare.GreaterThanOrEqualOp;
import com.pplive.pike.expression.compare.LessThanOp;
import com.pplive.pike.expression.compare.LessThanOrEqualOp;
import com.pplive.pike.expression.compare.LikeOp;
import com.pplive.pike.expression.compare.NotEqualOp;
import com.pplive.pike.expression.unary.InverseOp;
import com.pplive.pike.expression.unary.IsNotNullOp;
import com.pplive.pike.expression.unary.IsNullOp;
import com.pplive.pike.expression.unary.NotOp;
import com.pplive.pike.function.builtin.BuiltinFunctions;
import com.pplive.pike.function.builtin.Convert;
import com.pplive.pike.function.builtin.IBuiltinFunctionParser;
import com.pplive.pike.metadata.Column;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.statement.select.SubSelect;


class ExpressionParser extends ConstantExpressionParser {

	private final PikeSqlParser _sqlParser;
	private final SchemaScope _schemaScope;
    private final ISizeAwareIterable<String> _callingChainFunctions;
	private final boolean _isAggregateFuncCall;
	
	public ExpressionParser(PikeSqlParser sqlParser, SchemaScope schemaScope, Expression expression) {
		this(sqlParser, schemaScope, expression, null, false);
	}
	
	public ExpressionParser(PikeSqlParser sqlParser, SchemaScope schemaScope, Expression expression, ISizeAwareIterable<String> callingChainFunctions, boolean isAggregateFuncCall) {
		super(expression);
		assert (sqlParser != null);
		assert expression != null;
		assert schemaScope != null;

		this._sqlParser = sqlParser;
		this._schemaScope = schemaScope;
        this._callingChainFunctions = callingChainFunctions;
		this._isAggregateFuncCall = isAggregateFuncCall;
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
	public void visit(Function function) {
		FunctionParser parser = new FunctionParser(this._sqlParser, this._schemaScope, function, this._callingChainFunctions, this._isAggregateFuncCall);
		try{
			this._parsedExpr = parser.parse();
		}
		catch(ParseErrorsException e){
			for(Exception err : e.getParseErrors())
				addError(err);
			this._parsedExpr = null;
			return;
		}
	}
	
/*	public void visit(InverseExpression inverseExpression) {
		inverseExpression.getExpression().accept(this);
		AbstractExpression paramExpr = this._parsedExpr;
		if (paramExpr == null){
			this._parsedExpr = null;
			return;
		}
		
		try{
			UnaryOpExpression expr = new UnaryOpExpression(InverseOp.Op, InverseOp.class, paramExpr);
			this._parsedExpr = expr;
		}
		catch(SemanticErrorException e){
			this._parseErrors.add(e);
			this._parsedExpr = null;
		}
	}*/

	public void visit(SignedExpression signedExpression) {
		signedExpression.getExpression().accept(this);
		AbstractExpression paramExpr = this._parsedExpr;
		if (paramExpr == null){
			this._parsedExpr = null;
			return;
		}

		if (signedExpression.getSign() == 45){
			try{
				UnaryOpExpression expr = new UnaryOpExpression(InverseOp.Op, InverseOp.class, paramExpr);
				this._parsedExpr = expr;
			}
			catch(SemanticErrorException e){
				this._parseErrors.add(e);
				this._parsedExpr = null;
			}
		}
	}

	
	public void visit(JdbcParameter jdbcParameter) {
		addError(new SemanticErrorException("Dynamic parameter (?) in statement is not supported."));
		this._parsedExpr = null;
	}

	public void visit(Parenthesis parenthesis) {
		Expression expr = parenthesis.getExpression();
		assert expr != null;
		expr.accept(this);
		if (parenthesis.isNot()) {
			this._parsedExpr = new UnaryOpExpression(NotOp.Op, NotOp.class, this._parsedExpr);
		}
	}
	
	private void visitBinaryExpr(String operator, BinaryExpression binaryExpr, Class<?> opType) {
		assert binaryExpr != null;
		assert opType != null;
		
		this._parsedExpr = null;
		
		binaryExpr.getLeftExpression().accept(this);
		AbstractExpression left = this._parsedExpr;
		
		binaryExpr.getRightExpression().accept(this);
		AbstractExpression right = this._parsedExpr;

		if (left == null || right == null){
			this._parsedExpr = null;
			return;
		}
		
		try{
			BinaryOpExpression expr = new BinaryOpExpression(operator, opType, left, right);
			this._parsedExpr = expr;
		}
		catch(SemanticErrorException e){
			this._parseErrors.add(e);
			this._parsedExpr = null;
			return;
		}
		
		// because of syntax restriction, we cannot directly write: where XXX, have to write like: XXX == Boolean(1)
		// so here optimize for: XXX == Boolean(1), Boolean(1) == XXX, XXX <> Boolean(0), Boolean(0) <> XXX
		if (equalToRightTrue(left, right, opType) || notEqualToRightFalse(left, right, opType)) {
			this._parsedExpr = left;
		}
		else if (equalToLeftTrue(left, right, opType) || notEqualToLeftFalse(left, right, opType)) {
			this._parsedExpr = right;
		}

		if (binaryExpr.isNot()){
			this._parsedExpr = new UnaryOpExpression(NotOp.Op, NotOp.class, this._parsedExpr);
		}
	}
	
	private static boolean equalToRightTrue(AbstractExpression left, AbstractExpression right, Class<?> opType) {
		if (opType != EqualToOp.class)
			return false;
		if (right instanceof ConstantExpression && right.exprType() == Boolean.class) {
			Boolean val = (Boolean)right.eval(null);
			return val != null && val.booleanValue();
		}
		return false;
	}
	
	private static boolean equalToLeftTrue(AbstractExpression left, AbstractExpression right, Class<?> opType) {
		if (opType != EqualToOp.class)
			return false;
		if (left instanceof ConstantExpression && left.exprType() == Boolean.class) {
			Boolean val = (Boolean)left.eval(null);
			return val != null && val.booleanValue();
		}
		return false;
	}
	
	private static boolean notEqualToRightFalse(AbstractExpression left, AbstractExpression right, Class<?> opType) {
		if (opType != NotEqualOp.class)
			return false;
		if (right instanceof ConstantExpression && right.exprType() == Boolean.class) {
			Boolean val = (Boolean)right.eval(null);
			return val != null && val.booleanValue() == false;
		}
		return false;
	}
	
	private static boolean notEqualToLeftFalse(AbstractExpression left, AbstractExpression right, Class<?> opType) {
		if (opType != NotEqualOp.class)
			return false;
		if (left instanceof ConstantExpression && left.exprType() == Boolean.class) {
			Boolean val = (Boolean)left.eval(null);
			return val != null && val.booleanValue() == false;
		}
		return false;
	}
	
	public void visit(Addition addition) {
		visitBinaryExpr(AddOp.Op, addition, AddOp.class);
	}
	
	public void visit(Division division) {
		visitBinaryExpr(DivisionOp.Op, division, DivisionOp.class);
	}
	
	public void visit(Multiplication multiplication) {
		visitBinaryExpr(MultiOp.Op, multiplication, MultiOp.class);
	}
	
	public void visit(Subtraction subtraction) {
		visitBinaryExpr(SubtractOp.Op, subtraction, SubtractOp.class);
	}
	
	public void visit(AndExpression andExpression) {
		this._parsedExpr = null;
		
		andExpression.getLeftExpression().accept(this);
		AbstractExpression left = this._parsedExpr;
		
		andExpression.getRightExpression().accept(this);
		AbstractExpression right = this._parsedExpr;

		if (left == null || right == null){
			this._parsedExpr = null;
			return;
		}
		
		try{
			this._parsedExpr = new com.pplive.pike.expression.AndExpression(left, right);
		}
		catch(SemanticErrorException e){
			this._parseErrors.add(e);
			this._parsedExpr = null;
			return;
		}
		
		if (andExpression.isNot()){
			this._parsedExpr = new UnaryOpExpression(NotOp.Op, NotOp.class, this._parsedExpr);
		}
	}
	
	public void visit(OrExpression orExpression) {
		this._parsedExpr = null;
		
		orExpression.getLeftExpression().accept(this);
		AbstractExpression left = this._parsedExpr;
		
		orExpression.getRightExpression().accept(this);
		AbstractExpression right = this._parsedExpr;

		if (left == null || right == null){
			this._parsedExpr = null;
			return;
		}
		
		try{
			this._parsedExpr = new com.pplive.pike.expression.OrExpression(left, right);
		}
		catch(SemanticErrorException e){
			this._parseErrors.add(e);
			this._parsedExpr = null;
			return;
		}
		
		if (orExpression.isNot()){
			this._parsedExpr = new UnaryOpExpression(NotOp.Op, NotOp.class, this._parsedExpr);
		}
	}
	
	public void visit(Between between) {
		GreaterThanEquals left = new GreaterThanEquals();
		left.setLeftExpression(between.getLeftExpression());
		left.setRightExpression(between.getBetweenExpressionStart());
		
		MinorThanEquals right = new MinorThanEquals();
		right.setLeftExpression(between.getLeftExpression());
		right.setRightExpression(between.getBetweenExpressionEnd());
		
		AndExpression andExpr = new AndExpression(left, right);
		if (between.isNot())
			andExpr.setNot();
		visit(andExpr);
	}
	
	public void visit(EqualsTo equalsTo) {
		visitBinaryExpr(EqualToOp.Op, equalsTo, EqualToOp.class);
	}
	
	public void visit(NotEqualsTo notEqualsTo) {
		visitBinaryExpr(NotEqualOp.Op, notEqualsTo, NotEqualOp.class);
	}
	
	public void visit(GreaterThan greaterThan) {
		visitBinaryExpr(GreaterThanOp.Op, greaterThan, GreaterThanOp.class);
	}
	
	public void visit(GreaterThanEquals greaterThanEquals) {
		visitBinaryExpr(GreaterThanOrEqualOp.Op, greaterThanEquals, GreaterThanOrEqualOp.class);
	}

    private static boolean not(boolean expr) { return !expr; }

	public void visit(InExpression inExpression) {
        this._parsedExpr = null;
        inExpression.getLeftExpression().accept(this);
        AbstractExpression leftExpr = this._parsedExpr;

        final List<AbstractExpression> inExprs = new ArrayList<AbstractExpression>();

        inExpression.getRightItemsList().accept(new ItemsListVisitor() {
            @Override
            public void visit(SubSelect subSelect) {
                addError(new UnsupportedOperationException("IN ( <subquery> ) is not supported yet."));
            }

			@Override
			public void visit(MultiExpressionList multiExpressionList) {
				addError(new UnsupportedOperationException("IN ( multi expression ) is not supported yet."));
			}

            @Override
            public void visit(ExpressionList expressionList) {
                for(Expression expr : expressionList.getExpressions()) {
                    ExpressionParser.this._parsedExpr = null;
                    expr.accept(ExpressionParser.this);
                    final AbstractExpression candidate = ExpressionParser.this._parsedExpr;
                    if (candidate != null) {
                        if (not( ExpressionAggregateCallInspector.containsAggregateCall(candidate) )) {
                            inExprs.add(candidate);
                        }
                        else {
                            addError(new SemanticErrorException("IN ( ... ) cannot contain aggregation function call."));
                        }
                    }
                }
            }
		});

        this._parsedExpr = new com.pplive.pike.expression.InExpression(leftExpr, inExprs);
	}
	
	public void visit(IsNullExpression isNullExpression) {
		isNullExpression.getLeftExpression().accept(this);
		AbstractExpression paramExpr = this._parsedExpr;
		if (paramExpr == null){
			this._parsedExpr = null;
			return;
		}
		
		try{
			UnaryOpExpression expr;
			if (isNullExpression.isNot())
				expr = new UnaryOpExpression(IsNotNullOp.Op, IsNotNullOp.class, paramExpr);
			else
				expr = new UnaryOpExpression(IsNullOp.Op, IsNullOp.class, paramExpr);
			this._parsedExpr = expr;
		}
		catch(SemanticErrorException e){
			this._parseErrors.add(e);
			this._parsedExpr = null;
		}
	}
	
	public void visit(LikeExpression likeExpression) {
		visitBinaryExpr(LikeOp.Op, likeExpression, LikeOp.class);
	}
	
	public void visit(MinorThan minorThan) {
		visitBinaryExpr(LessThanOp.Op, minorThan, LessThanOp.class);
	}
	
	public void visit(MinorThanEquals minorThanEquals) {
		visitBinaryExpr(LessThanOrEqualOp.Op, minorThanEquals, LessThanOrEqualOp.class);
	}
	
	public void visit(net.sf.jsqlparser.schema.Column tableColumn) {
		this._parsedExpr = null;
		
		final String columnName = tableColumn.getColumnName();
		final net.sf.jsqlparser.schema.Table table = tableColumn.getTable();
		assert table != null;
		String tableName = "";
		if (table.getSchemaName() != null) {
			String msg = String.format("%s: table with schema name is not supported", tableColumn.getFullyQualifiedName());
			addError(new SemanticErrorException(msg));
			this._parsedExpr = null;
			return;
		}
		Column col;
		if (table.getName() != null) {
			tableName = table.getName();
			col = this._schemaScope.findColumn(tableName, columnName);
			if (col == null){
				String msg = String.format("table column %s.%s not found.", tableName, columnName);
				addError(new SemanticErrorException(msg));
				this._parsedExpr = null;
				return;
			}
			// ensure tableName is correct even without ignoring case, important in topology generate stage
			tableName = this._schemaScope.findTable(tableName).getName();
		}
		else {
			if (this._schemaScope.hasColumn(columnName) == false){
				addError(new SemanticErrorException(String.format("column %s not found.", columnName)));
				this._parsedExpr = null;
				return;
			}
			if (this._schemaScope.hasUniqueColumn(columnName) == false){
				addError(new SemanticErrorException(String.format("column %s exists in 2+ tables, need specify table name", columnName)));
				this._parsedExpr = null;
				return;
			}
			col = this._schemaScope.getFirstColumn(columnName).getColumn();
		}
		
		// use col.getName() instead of columnName, ensure it's correct even case-sensitive, important in topology generate stage
		ColumnExpression columnExpr = new ColumnExpression(tableName, col.getColumnType(), col.getName());

		if (tableName.isEmpty())
			tableName = this._schemaScope.getFirstColumn(columnName).tableName().value();
		RelationalExprSchema tableOfColumn = this._schemaScope.findTable(tableName);
		assert tableOfColumn != null;
		if (tableOfColumn.needConvert(col.getName()) == false) {
			this._parsedExpr = columnExpr;
		}
		else {
			// 
			// after adding new interfaces/classes and improving trident ChainedAggregatorDeclarer/ChainedAggregatorImpl,
			// in aggregation we have extra final step to convert aggregate state class to final result type.
			// so here should be actually unreachable.
			assert false;
			
			String funcName = columnExpr.exprType().getSimpleName();
			IBuiltinFunctionParser parser = BuiltinFunctions.getFunctionParser(funcName);
			assert parser != null;
			AbstractExpression convertExpr = parser.parse(funcName, Arrays.asList((AbstractExpression)columnExpr));
			this._parsedExpr = convertExpr;
		}
	}
	
	public void visit(SubSelect subSelect) {

		if (this._callingChainFunctions != null && this._callingChainFunctions.size() > 0) {
            String func = SizeAwareIterable.last(this._callingChainFunctions);
			String msg = String.format("%s() cannot contain subquery as parameter", func);
			addError(new SemanticErrorException(msg));
			this._parsedExpr = null;
			return;
		}
		addError(new UnsupportedOperationException("subquery expression is not implemented yet."));
		this._parsedExpr = null;
	}
	
	private static class IfExpr implements Expression
	{
		public final Expression Condition;
		public final Expression TrueResult;
		public final Expression FalseResult;
		
		public IfExpr(Expression cond, Expression trueResult, Expression falseResult) {
			this.Condition = cond;
			this.TrueResult = trueResult;
			this.FalseResult = falseResult;
		}
		
		@Override
		public void accept(ExpressionVisitor visitor) {
			assert visitor instanceof ExpressionParser;
			((ExpressionParser)visitor).visitIfExpr(this);
		}
	}
	
	private void visitIfExpr(IfExpr ifExpr) {
		assert ifExpr != null;
		
		ifExpr.Condition.accept(this);
		AbstractExpression condition = this._parsedExpr;
		
		ifExpr.TrueResult.accept(this);
		AbstractExpression trueResult = this._parsedExpr;
		if (trueResult != null && ExpressionAggregateCallInspector.containsAggregateCall(trueResult)){
			addError(new SemanticErrorException("CASE expression result cannot contain aggregate function call"));
		}
		
		ifExpr.FalseResult.accept(this);
		AbstractExpression falseResult = this._parsedExpr;
		if (falseResult != null && ExpressionAggregateCallInspector.containsAggregateCall(falseResult)){
			addError(new SemanticErrorException("CASE expression result cannot contain aggregate function call"));
		}
		
		if (condition == null || trueResult == null || falseResult == null){
			this._parsedExpr = null;
			return;
		}
		
		if (IfExpression.isCompatibleType(trueResult, falseResult) == false) {
			String msg = String.format("CASE expression result type inconsistent, one branch is %s, another branch is %s",
										trueResult.exprType().getSimpleName(), falseResult.exprType().getSimpleName());
			addError(new SemanticErrorException(msg));
			this._parsedExpr = null;
			return;
		}
		
		if (isIfThenTrueElseFalse(trueResult, falseResult)) {
			if (condition.exprType() != Boolean.class) {
				condition = new Convert.Parser().parse("Boolean", Arrays.asList(condition));
			}
			this._parsedExpr = condition;
		}
		else {
			IfExpression expr = new IfExpression(condition, trueResult, falseResult);
			this._parsedExpr = expr;
		}
	}
	
	private static boolean isIfThenTrueElseFalse(AbstractExpression trueResult, AbstractExpression falseResult) {
		if (trueResult instanceof ConstantExpression && trueResult.exprType() == Boolean.class
			&& falseResult instanceof ConstantExpression && falseResult.exprType() == Boolean.class) {
			Boolean thenVal = (Boolean)trueResult.eval(null);
			Boolean elseVal = (Boolean)falseResult.eval(null);
			return thenVal != null && thenVal.booleanValue() == true
					&& elseVal != null && elseVal.booleanValue() == false;
		}
		return false;
	}
	
	private static Expression createIfCondition(Expression switchExpr, Expression whenExpr) {
		if (switchExpr == null)
			return whenExpr;
		EqualsTo equalsTo = new EqualsTo();
		equalsTo.setLeftExpression(switchExpr);
		equalsTo.setRightExpression(whenExpr);
		return equalsTo;
	}
	
	public void visit(CaseExpression caseExpression) {
		Expression switchExpr = caseExpression.getSwitchExpression();
		Expression elseExpr = caseExpression.getElseExpression();
		if (elseExpr == null) {
			elseExpr = new NullValue();
		}

		List<Expression> clauses = caseExpression.getWhenClauses();

		WhenClause[] whenClauses = clauses.toArray(new WhenClause[0]);
		assert whenClauses.length > 0;
		IfExpr ifExpr = null;
		for(int n = whenClauses.length - 1; n >= 0; n -= 1) {
			WhenClause whenClause = whenClauses[n];
			Expression condExpr = createIfCondition(switchExpr, whenClause.getWhenExpression());
			ifExpr = new IfExpr(condExpr, whenClause.getThenExpression(), elseExpr);
			elseExpr = ifExpr;
		}
		assert ifExpr != null;
		visitIfExpr(ifExpr);
	}
	
	public void visit(WhenClause whenClause) {
		throw new IllegalStateException("bug: program run incorrectly, should never happen");
	}
	
	public void visit(ExistsExpression existsExpression) {
		addError(new UnsupportedOperationException("EXISTS is not implemented yet."));
		this._parsedExpr = null;
	}
	
	public void visit(AllComparisonExpression allComparisonExpression) {
		addError(new UnsupportedOperationException("subquery (ALL ...) is not implemented yet."));
		this._parsedExpr = null;
	}
	
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		addError(new UnsupportedOperationException("subquery (ANY ...) is not implemented yet."));
		this._parsedExpr = null;
	}
	
	public void visit(Concat concat) {
		addError(new UnsupportedOperationException("CONCAT (||) is not implemented yet."));
		this._parsedExpr = null;
	}
	
	public void visit(Matches matches) {
		addError(new UnsupportedOperationException("MATCH (@@) is not implemented yet."));
		this._parsedExpr = null;
	}
	
	public void visit(BitwiseAnd bitwiseAnd) {
		addError(new UnsupportedOperationException("BitwiseAnd (&) is not implemented yet."));
		this._parsedExpr = null;
	}
	
	public void visit(BitwiseOr bitwiseOr) {
		addError(new UnsupportedOperationException("BitwiseOr (|) is not implemented yet."));
		this._parsedExpr = null;
	}
	
	public void visit(BitwiseXor bitwiseXor) {
		addError(new UnsupportedOperationException("BitwiseXor (^) is not implemented yet."));
		this._parsedExpr = null;
	}
}
