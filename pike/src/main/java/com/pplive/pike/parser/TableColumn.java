package com.pplive.pike.parser;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.Immutable;
import com.pplive.pike.metadata.Column;

@Immutable
final class TableColumn {
	private final CaseIgnoredString _tableName;
	private final CaseIgnoredString _column;

	public TableColumn(String table, String col) {
		this._tableName = new CaseIgnoredString(table);
		this._column = new CaseIgnoredString(col);
	}

	public TableColumn(CaseIgnoredString table, CaseIgnoredString col) {
		this._tableName = table;
		this._column = col;
	}
	
	public CaseIgnoredString tableName() {
		return this._tableName;
	}
	
	public CaseIgnoredString columnName() {
		return this._column;
	}
}
