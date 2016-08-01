package com.pplive.pike.parser;

import com.pplive.pike.base.SortOrder;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ColumnExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;

import java.util.ArrayList;
import java.util.List;

class TopGroupByColumnsParser {

	private final PikeSqlParser _sqlParser;
	private ArrayList<ColumnExpression> _columns;

	public Iterable<ColumnExpression> getTopGroupByColumns() {
		return this._columns;
	}

	public int getTopGroupByColumn() {
		return this._columns.size();
	}

	private final SchemaScope _schemaScope;

	public TopGroupByColumnsParser(PikeSqlParser sqlParser, SchemaScope scope){
		assert sqlParser != null;
		assert scope != null;

		this._sqlParser = sqlParser;
		this._schemaScope = scope;
	}
	
	public Iterable<ColumnExpression> parse(List<Expression> topGroupByCols, Iterable<OrderByColumn> orderByCols) {
		this._columns = new ArrayList<ColumnExpression>(topGroupByCols.size());
		for(Expression expr : topGroupByCols) {
            if ((expr instanceof net.sf.jsqlparser.schema.Column) == false) {
                throw new SemanticErrorException("TOP GROUP BY can contain only simple column names, like: TOP GROUP BY a , b, ...");
            }

            ExpressionParser parser = new ExpressionParser(this._sqlParser, this._schemaScope, expr);
            AbstractExpression parsedExpr = parser.parse();
            assert parsedExpr instanceof ColumnExpression;
            ColumnExpression columnExpr = (ColumnExpression)parsedExpr;

            if (not(isInOrderByColumns(columnExpr, orderByCols))) {
                String msg = String.format("TOP GROUP BY column '%s' does not appear in ORDER BY ... columns", columnExpr.getColumnName());
                throw new SemanticErrorException(msg);
            }

            this._columns.add(columnExpr);
		}

		return this._columns;
	}

    private static boolean not(boolean expr) { return !expr; }

    private static boolean isInOrderByColumns(ColumnExpression columnExpr, Iterable<OrderByColumn> orderByCols) {
        if (orderByCols == null)
            return false;
        for(OrderByColumn c : orderByCols) {
            if (columnExpr.getColumnName().equalsIgnoreCase(c.columnExpr().getColumnName()))
                return true;
        }
        return false;
    }

}
