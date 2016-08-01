package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.List;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.SortOrder;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.metadata.Column;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;

class OrderByColumnsParser implements OrderByVisitor {
	
	private final PikeSqlParser _sqlParser;
	private ArrayList<OrderByColumn> _columns;
	
	public Iterable<OrderByColumn> getOrderByColumns() {
		return this._columns;
	}
	
	public int getOrderByColumnCount() {
		return this._columns.size();
	}

	private final SchemaScope _schemaScope;
	private final List<OrderByElement> _orderByElements;
	
	public OrderByColumnsParser(PikeSqlParser sqlParser, SchemaScope scope, List<OrderByElement> cols){
		assert sqlParser != null;
		assert scope != null;
		assert cols != null;
		
		this._sqlParser = sqlParser;
		this._schemaScope = scope;
		this._orderByElements = cols;
	}
	
	public Iterable<OrderByColumn> parse() {
		this._columns = new ArrayList<OrderByColumn>(this._orderByElements.size());
		for(OrderByElement orderByItem : this._orderByElements) {
			orderByItem.accept(this);
		}
		
		return this._columns;
	}

	public void visit(OrderByElement orderBy) {
		Expression expr = orderBy.getExpression();
		if ((expr instanceof net.sf.jsqlparser.schema.Column) == false) {
			throw new SemanticErrorException("ORDER BY can contain only simple column names, like: ORDER BY a ASC, b DESC, ...");
		}
		
		ExpressionParser parser = new ExpressionParser(this._sqlParser, this._schemaScope, expr);
		AbstractExpression parsedExpr = parser.parse();
		assert parsedExpr instanceof ColumnExpression;
		ColumnExpression columnExpr = (ColumnExpression)parsedExpr;
		if (Comparable.class.isAssignableFrom(columnExpr.exprType()) == false){
			String msg = String.format("ORDER BY: column %s type is %s, it's not comparable", columnExpr.getColumnName(), columnExpr.exprType().getSimpleName());
			throw new SemanticErrorException(msg);
		}
		
		SortOrder order = orderBy.isAsc() ? SortOrder.Ascending : SortOrder.Descending;
		this._columns.add(new OrderByColumn(columnExpr, order));
	}
	
}
