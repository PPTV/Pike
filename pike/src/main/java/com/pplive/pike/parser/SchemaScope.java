package com.pplive.pike.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.Immutable;
import com.pplive.pike.metadata.Column;

@Immutable
final class SchemaScope {
	@Immutable
	public static class TableColumn {
		private final CaseIgnoredString _tableName;
		private final Column _column;
		public TableColumn(CaseIgnoredString table, Column col) {
			this._tableName = table;
			this._column = col;
		}
		
		public CaseIgnoredString tableName() {
			return this._tableName;
		}
		
		public Column getColumn() {
			return this._column;
		}
	}
	
	private final Map<CaseIgnoredString, RelationalExprSchema> _tables = new HashMap<CaseIgnoredString, RelationalExprSchema>();
	
	private final Map<CaseIgnoredString, TableColumn[]> _columns = new HashMap<CaseIgnoredString, TableColumn[]>();
	
	private final SchemaScope _parent;
	
	public SchemaScope(RelationalExprSchema table) {
		assert table != null;
		this._parent = null;
		addTable(table);
	}
	
	public SchemaScope(Iterable<RelationalExprSchema> tables) {
		assert tables != null;
		this._parent = null;
		for(RelationalExprSchema t : tables) {
			addTable(t);
		}
	}
	
	private SchemaScope(SchemaScope parent, RelationalExprSchema table) {
		assert parent != null;
		assert table != null;
		this._parent = parent;
		addTable(table);
	}
	
	private SchemaScope(SchemaScope parent, Iterable<RelationalExprSchema> tables) {
		assert parent != null;
		assert tables != null;
		this._parent = parent;
		for(RelationalExprSchema t : tables) {
			addTable(t);
		}
	}
	
	private void addTable(RelationalExprSchema table) {
		assert table != null;
		CaseIgnoredString tableName = new CaseIgnoredString(table.getName());
		this._tables.put(tableName, table);
		Column[] columns = table.getColumns();
		for(Column col : columns) {
			addTableColumn(table.getName(), col);
		}
	}
	
	private void addTableColumn(String table, Column column) {
		assert table != null && column != null;
		
		TableColumn tableCol = new TableColumn(new CaseIgnoredString(table), column);
		CaseIgnoredString colKey = new CaseIgnoredString(column.getName());
		TableColumn[] cols = this._columns.get(colKey);
		if (cols == null) {
			cols = new TableColumn[]{tableCol};
		}
		else {
			cols = addNewTableColumn(cols, tableCol);
		}
		this._columns.put(colKey, cols);
	}
	
	private static TableColumn[] addNewTableColumn(TableColumn[] old, TableColumn col) {
		assert old != null && col != null;
		TableColumn[] result = Arrays.copyOf(old, old.length + 1);
		result[old.length] = col;
		return result;
	}
	
	public boolean isRootScope() {
		return this._parent == null;
	}
	
	public SchemaScope enterNewChildScope(RelationalExprSchema table) {
		return new SchemaScope(this, table);
	}
	
	public SchemaScope enterNewChildScope(Iterable<RelationalExprSchema> tables) {
		return new SchemaScope(this, tables);
	}
	
	public SchemaScope backToParentScope() {
		return this._parent;
	}
	
	public RelationalExprSchema findTable(String tableName) {
		return findTable(new CaseIgnoredString(tableName));
	}
	
	public RelationalExprSchema findTable(CaseIgnoredString tableName) {
		return this._tables.get(tableName);
	}
	
	public Column findColumn(String tableName, String columnName) {
		return findColumn(new CaseIgnoredString(tableName), new CaseIgnoredString(columnName));
	}
	
	public Column findColumn(CaseIgnoredString tableName, CaseIgnoredString columnName) {
		RelationalExprSchema t = findTable(tableName);
		if (t == null)
			return null;
		return t.getColumn(columnName.value());
	}

	public boolean hasColumn(String columnName) {
		return hasColumn(new CaseIgnoredString(columnName));
	}

	public boolean hasColumn(CaseIgnoredString columnName) {
		TableColumn[] cols = this._columns.get(columnName);
		return cols != null && cols.length > 0;
	}

	public boolean hasUniqueColumn(String columnName) {
		return hasUniqueColumn(new CaseIgnoredString(columnName));
	}

	public boolean hasUniqueColumn(CaseIgnoredString columnName) {
		TableColumn[] cols = this._columns.get(columnName);
		return cols != null && cols.length == 1;
	}

	public TableColumn getFirstColumn(String columnName) {
		return getFirstColumn(new CaseIgnoredString(columnName));
	}

	public TableColumn getFirstColumn(CaseIgnoredString columnName) {
		TableColumn[] cols = this._columns.get(columnName);
		if (cols == null || cols.length == 0)
			return null;
		return cols[0];
	}

}
