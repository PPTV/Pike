package com.pplive.pike.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.Immutable;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;

class GlobalSchemaScope {
	
	private final SchemaScope _scope;
	public SchemaScope schemaScope(){
		return this._scope;
	}
	
	private final Map<CaseIgnoredString, RelationalExprOperator> _tableOps = new HashMap<CaseIgnoredString, RelationalExprOperator>();
	
	public GlobalSchemaScope(RelationalExprSchema[] tables, RelationalExprOperator[] ops) {
		this(Arrays.asList(tables), Arrays.asList(ops));
	}
	
	public GlobalSchemaScope(Iterable<RelationalExprSchema> tables, Iterable<RelationalExprOperator> ops) {
		assert tables != null;
		assert ops != null;
				
		this._scope = new SchemaScope(tables);
		for(RelationalExprOperator op : ops) {
			CaseIgnoredString tableName = new CaseIgnoredString(op.getOutputSchema().getName());
			this._tableOps.put(tableName, op);
		}
	}
	
	public RelationalExprOperator getTableOp(String tableName) {
		return getTableOp(new CaseIgnoredString(tableName));
	}
		
	public RelationalExprOperator getTableOp(CaseIgnoredString tableName) {
		return this._tableOps.get(tableName);
	}
		
	public Table findTable(String tableName) {
		return findTable(new CaseIgnoredString(tableName));
	}
	
	public Table findTable(CaseIgnoredString tableName) {
		return this._scope.findTable(tableName);
	}
	
	public Column findColumn(String tableName, String columnName) {
		return this._scope.findColumn(tableName, columnName);
	}
	
	public Column findColumn(CaseIgnoredString tableName, CaseIgnoredString columnName) {
		return this._scope.findColumn(tableName, columnName);
	}

	public boolean hasUniqueColumn(String columnName) {
		return this._scope.hasUniqueColumn(columnName);
	}

	public boolean hasUniqueColumn(CaseIgnoredString columnName) {
		return this._scope.hasUniqueColumn(columnName);
	}

	public SchemaScope.TableColumn getFirstColumn(String columnName) {
		return this._scope.getFirstColumn(columnName);
	}

	public SchemaScope.TableColumn getFirstColumn(CaseIgnoredString columnName) {
		return this._scope.getFirstColumn(columnName);
	}

}
