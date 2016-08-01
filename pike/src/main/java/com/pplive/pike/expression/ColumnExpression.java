package com.pplive.pike.expression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.parser.SemanticErrorException;
import com.pplive.pike.util.TupleUtil;

import storm.trident.tuple.TridentTuple;

public final class ColumnExpression extends AbstractExpression {
	
	private static final long serialVersionUID = 6730683349836186079L;
	private final String tableName;
	private final String columnName;
	public String getColumnName() { return columnName; }
	private final ColumnType _columnType;
	public ColumnType getColumnType() { return this._columnType; }
	
	private int _cachedColumnIndex = -1;

	public ColumnExpression(ColumnType columnType, String columnName){
		this("", columnType, columnName);
	}
	
	public ColumnExpression(String tableName, ColumnType columnType, String columnName){
		this.tableName = tableName;
		this.columnName = columnName;
		this._columnType = columnType;
	}
	
	public ColumnExpression(Column column){
		this("", column.getColumnType(), column.getName());
	}
	
	@Override
	public Object eval(TridentTuple tuple) {
		if (this._cachedColumnIndex >= 0) {
			assert this._cachedColumnIndex < tuple.size();
			if (this._cachedColumnIndex < tuple.size())
				return tuple.getValue(this._cachedColumnIndex);
			// todo: need log warning, if come here something must be wrong 
		}
		
		this._cachedColumnIndex = TupleUtil.findIndex(tuple, this.columnName);
		if (this._cachedColumnIndex < 0){
			return null;
		}
		
		Object o = tuple.getValue(this._cachedColumnIndex);
		// check and convert to expected type
		if (o != null && this._columnType != ColumnType.Unknown) {
			Class<?> t = o.getClass();
			if (t == exprType())
				return o;
			// todo: need log warning, something must be wrong 
			// todo: convert
		}
		return o;
	}
	
	@Override
	public Object visit(Object context, IExpressionVisitor visitor) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toString(){
		if (this.tableName != null && this.tableName.isEmpty() == false){
			return this.tableName + "." + this.columnName;
		}
		else{
			return this.columnName;
		}
	}
	
	@Override
	public Class<?> exprType() {
		Class<?> t = ColumnType.convert(this._columnType);
		if (t != null)
			return t;
		throw new SemanticErrorException(String.format("unsupported column type: %s", this._columnType));
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o == this)
			return true;
		if (o.getClass() != ColumnExpression.class)
			return false;
		ColumnExpression other = (ColumnExpression)o;
		assert this.tableName != null;
		if (this.tableName.equalsIgnoreCase(other.tableName) == false)
			return false;
		assert this.columnName != null;
		if (this.columnName.equalsIgnoreCase(other.columnName) == false)
			return false;
		return this._columnType == other._columnType;
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
