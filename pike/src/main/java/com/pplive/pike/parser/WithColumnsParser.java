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

class WithColumnsParser implements SelectItemVisitor {
	
	private ArrayList<CaseIgnoredString> _columnNames;
	
	public ArrayList<CaseIgnoredString> getColumnNames() {
		return this._columnNames;
	}
	
	public void parseWithColumns(List<SelectItem> cols) {
		assert cols != null;
		
		this._columnNames = new ArrayList<CaseIgnoredString>(cols.size());
		for(SelectItem selectItem : cols) {
			selectItem.accept(this);
		}
	}

	public void visit(AllColumns allColumns) {
		throw new SemanticErrorException("WITH can contain only simple column names, like: WITH A(a, b, c) AS (...)");
	}
	
	public void visit(AllTableColumns allTableColumns) {
		throw new SemanticErrorException("WITH can contain only simple column names, like: WITH A(a, b, c) AS (...)");
	}
	
	public void visit(SelectExpressionItem selectExpressionItem) {
		Expression expr = selectExpressionItem.getExpression();
		if ((expr instanceof Column) == false) {
			throw new SemanticErrorException("WITH can contain only simple column names, like: WITH A(a, b, c) AS (...)");
		}
		Column column = (Column)expr;
		if (column.getTable() != null && column.getTable().getName() != null){
			throw new SemanticErrorException("WITH can contain only simple column names, like: WITH A(a, b, c) AS (...)");
		}
		this._columnNames.add(new CaseIgnoredString(column.getColumnName()));
	}
	
}
