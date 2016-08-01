package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.expression.ExpressionRefColumnsGetter;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableDataSource;
import com.pplive.pike.util.CollectionUtil;

public class TransformOp extends RelationalExprOperator {
	
	protected ArrayList<TransformField> _nonAggregateFields;
	public Iterable<TransformField> getNonAggregateFields() {
		return this._nonAggregateFields;
	}
	public int getNonAggregateCount() {
		return this._nonAggregateFields.size();
	}
	
	protected HashMap<CaseIgnoredString, Column> _projectColumns;
	public Iterable<Column> getProjectColumns() {
		return this._projectColumns.values();
	}
	public int getProjectColumnCount() {
		return this._projectColumns.values().size();
	}

	public TransformOp(RelationalExprOperator child, Iterable<TransformField> nonAggregateFields) {
		if (child == null)
			throw new IllegalArgumentException("child cannot be null");
		if (nonAggregateFields == null)
			throw new IllegalArgumentException("nonAggregateFields cannot be null");
		
		this._nonAggregateFields = CollectionUtil.copyArrayList(nonAggregateFields);
		
		setProjectColumns(nonAggregateFields);
		setOutputSchema(child, nonAggregateFields);
		child.setParent(this);
		this._child = child;
	}
	
	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}
	
	private void setProjectColumns(Iterable<TransformField> nonAggregateFields) {
		this._projectColumns = new HashMap<CaseIgnoredString, Column>();
		for(TransformField f : nonAggregateFields) {
			Map<CaseIgnoredString, Column> cols = ExpressionRefColumnsGetter.getAllReferencedColumns(f.getExpression());
			assert cols != null;
			this._projectColumns.putAll(cols);
		}
	}
	
	private void setOutputSchema(RelationalExprOperator child, Iterable<TransformField> nonAggregateFields) {
		assert child != null;
		assert nonAggregateFields != null;
		
		ArrayList<Column> outputColumns = new ArrayList<Column>();
		ArrayList<String> needConvertColumns = new ArrayList<String>();
		for(TransformField f : nonAggregateFields) {
			CaseIgnoredString colName = f.getAlias();
			Column col = new Column(colName.value(), "", f.getExpression().exprType());
			outputColumns.add(col);
			if (f.getExpression().needConvertResultToExprType()){
				needConvertColumns.add(colName.value());
			}
		}
		
		TableDataSource tableDataSource = child.getOutputSchema().getTableDataSource();
		CaseIgnoredString tableName = InternalTableName.genTableName(child.getOutputSchema().getName());
		this._outputSchema = new RelationalExprSchema(tableDataSource, tableName.value(), "", outputColumns.toArray(new Column[0]), needConvertColumns);
	}
	
	@Override
	public String toExplainString() {
		return this._child.toExplainString() 
				+ String.format("Transform:%n")
				+ String.format("\tinput table: %s%n", this._child.getOutputSchema().getName())
				+ String.format("\tproject columns: %s%n",  getColumnsString(this._projectColumns.values()))
				+ String.format("\ttransform items: %s%n", getTransformFieldsExprString(this._nonAggregateFields))
				+ String.format("\toutput table: %s(%s)%n", this._outputSchema.getName(), getTableColumns(this._outputSchema))
				;
	}
	
	protected static String getTransformFieldsExprString(ArrayList<TransformField> fields) {
		assert fields != null;
		StringBuilder sb = new StringBuilder();
		int n = 0;
		for(TransformField f : fields){
			n += 1;
			if (n > 1) sb.append(", ");
			sb.append(f.getExpression().toString());
		}
		return sb.toString();
	}
	
	protected static String getColumnsString(Iterable<Column> columns) {
		assert columns != null;
		StringBuilder sb = new StringBuilder();
		int n = 0;
		for(Column col : columns){
			n += 1;
			if (n > 1) sb.append(", ");
			sb.append(col.getName());
		}
		return sb.toString();
	}
	
	protected static String getTableColumns(RelationalExprSchema t) {
		assert t != null;
		StringBuilder sb = new StringBuilder();
		int n = 0;
		for(Column col : t.getColumns()){
			n += 1;
			if (n > 1) sb.append(", ");
			if (t.needConvert(col.getName())) 
				sb.append(String.format("%s(ToConvert) %s", col.getColumnType(), col.getName()));
			else
				sb.append(String.format("%s %s", col.getColumnType(), col.getName()));
		}
		return sb.toString();
	}
}


