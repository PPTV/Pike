package com.pplive.pike.parser;

import com.pplive.pike.base.Immutable;
import com.pplive.pike.base.SortOrder;
import com.pplive.pike.expression.ColumnExpression;

@Immutable
public final class OrderByColumn implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final ColumnExpression _columnExpr;
	
	public ColumnExpression columnExpr() {
		return this._columnExpr;
	}
	
	private final SortOrder _sortOrder;
	public SortOrder getSortOrder() {
		return this._sortOrder;
	}
	
	public OrderByColumn(ColumnExpression columnExpr, SortOrder order) {
		this._columnExpr = columnExpr;
		this._sortOrder = order;
	}
	
	@Override
	public String toString() {
		return String.format("%s %s", this._columnExpr,
					(this._sortOrder == SortOrder.Ascending ? "ASC" : "DESC"));
	}
}
