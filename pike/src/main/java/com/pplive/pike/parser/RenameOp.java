package com.pplive.pike.parser;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;

public class RenameOp extends RelationalExprOperator {
	
	private final CaseIgnoredString _alias;
	public CaseIgnoredString getAlias(){
		return this._alias;
	}

	public RenameOp(RelationalExprOperator child, String alias) {
		this(child, new CaseIgnoredString(alias));
	}
	
	public RenameOp(RelationalExprOperator child, CaseIgnoredString alias) {
		if (child == null)
			throw new IllegalArgumentException("child cannot be null");
		if (alias == null || alias.isEmpty())
			throw new IllegalArgumentException("alias cannot be null or empty");
		
		this._child = child;
		this._child.setParent(this);
		this._alias = alias;
		
		RelationalExprSchema childTable = child.getOutputSchema();
		Column[] columns = childTable.getColumns();
		Iterable<String> needConvertCols = childTable.getColumnsNeedConvert();
		RelationalExprSchema table = new RelationalExprSchema(childTable.getTableDataSource(), alias.value(), "", columns, needConvertCols);
		this._outputSchema = table;
	}
	
	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toExplainString() {
		return this._child.toExplainString()
				+ String.format("Rename:%n")
				+ String.format("\ttable: %s  -->  table: %s%n", this._child.getOutputSchema().getName(), this._outputSchema.getName());
	}
}
