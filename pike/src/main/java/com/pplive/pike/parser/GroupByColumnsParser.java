package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.List;

import com.pplive.pike.base.CaseIgnoredString;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

class GroupByColumnsParser {

	private final SchemaScope _schemaScope;
	private final List<Expression> _expressions;
	
	public GroupByColumnsParser(SchemaScope schemaScope, List<Expression> expressions) {
		assert schemaScope != null;
		assert expressions != null;
		
		this._schemaScope = schemaScope;
		this._expressions = expressions;
	}
	
	public List<TableColumn> parse() {
		ArrayList<TableColumn> cols = new ArrayList<TableColumn>(this._expressions.size());
		for(Expression expr : this._expressions){
			if ((expr instanceof Column) == false) {
				throw new UnsupportedOperationException("GROUP BY currently support only simple column names, like: GROUP BY a, t.b ...");
			}
			Column column = (Column)expr;
			CaseIgnoredString col = new CaseIgnoredString(column.getColumnName());
			CaseIgnoredString table;
			if (column.getTable() != null && column.getTable().getName() != null){
				if (column.getTable().getSchemaName() != null){
					String msg = String.format("GROUP BY error: column %s with schema name is not supported", column.getFullyQualifiedName());
					throw new UnsupportedOperationException(msg);
				}
				table = new CaseIgnoredString(column.getTable().getName());
			}
			else{
				table = new CaseIgnoredString("");
			}
			cols.add(new TableColumn(table, col));
		}
		return cols;
	}
	
}
