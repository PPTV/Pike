package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableDataSource;

public class ProjectOp extends RelationalExprOperator {
	
	public static boolean isOnlyProjection(Iterable<TransformField> nonAggregateFields) {
		if (nonAggregateFields == null)
			throw new IllegalArgumentException("nonAggregateFields cannot be null");
		
		HashSet<CaseIgnoredString> columns = new HashSet<CaseIgnoredString>(20);
		int fieldsCount = 0;
		for (TransformField f : nonAggregateFields) {
			fieldsCount += 1;
			AbstractExpression expr = f.getExpression();
			if (expr.getClass() != ColumnExpression.class){
				return false;
			}
			String col = ((ColumnExpression)expr).getColumnName();
			columns.add(new CaseIgnoredString(col));
		}
		return fieldsCount == columns.size();
	}

	protected ArrayList<ProjectField> _simpleColumnFields;
	public Iterable<ProjectField> getColumnFields() {
		return this._simpleColumnFields;
	}
	
	protected HashMap<CaseIgnoredString, Column> _projectColumns;
	public Iterable<Column> getProjectColumns() {
		return this._projectColumns.values();
	}
	public int getProjectColumnCount() {
		return this._projectColumns.size();
	}
	
	public ProjectOp(RelationalExprOperator child, Collection<ProjectField> simpleColumnFields) {
		if (child == null)
			throw new IllegalArgumentException("child cannot be null");
		if (simpleColumnFields == null)
			throw new IllegalArgumentException("simpleColumnFields cannot be null");
		
		this._simpleColumnFields = new ArrayList<ProjectField>(simpleColumnFields.size());
		this._simpleColumnFields.addAll(simpleColumnFields);
		
		setProjectColumns(simpleColumnFields);
		setOutputSchema(child, simpleColumnFields);
		child.setParent(this);
		this._child = child;
	}
	
	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}
	
	private void setProjectColumns(Collection<ProjectField> simpleColumnFields) {
		this._projectColumns = new HashMap<CaseIgnoredString, Column>(20);
		for(ProjectField f : simpleColumnFields) {
			ColumnExpression expr = f.getExpression();
			Column col = new Column(expr.getColumnName(), "", expr.exprType());
			this._projectColumns.put(new CaseIgnoredString(col.getName()), col);
		}
		
		if (this._simpleColumnFields.size() != this._projectColumns.size()){
			assert false;
			throw new IllegalStateException("simpleColumnFields some column twice or more");
		}
	}
	
	private void setOutputSchema(RelationalExprOperator child, Collection<ProjectField> simpleColumnFields) {
		assert child != null;
		assert simpleColumnFields != null;
		
		ArrayList<Column> outputColumns = new ArrayList<Column>();
		ArrayList<String> needConvertColumns = new ArrayList<String>();
		for(TransformField f : simpleColumnFields) {
			CaseIgnoredString colName = f.getAlias();
			assert f.getExpression() instanceof ColumnExpression;
			ColumnExpression expr = (ColumnExpression)f.getExpression();
			Column col = new Column(colName.value(), "", f.getExpression().exprType());
			outputColumns.add(col);
			if (f.getExpression().needConvertResultToExprType() || child.getOutputSchema().needConvert(expr.getColumnName())){
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
				+ "Project:\r\n"
				+ String.format("\tinput table: %s%n", this._child.getOutputSchema().getName())
				+ String.format("\tproject items: %s%n", getProjectFieldsExprString(this._simpleColumnFields))
				+ String.format("\toutput table: %s(%s)%n", this._outputSchema.getName(), getTableColumns(this._outputSchema));
	}
	
	private static String getProjectFieldsExprString(ArrayList<ProjectField> fields) {
		assert fields != null;
		StringBuilder sb = new StringBuilder();
		int n = 0;
		for(ProjectField f : fields){
			n += 1;
			if (n > 1) sb.append(", ");
			sb.append(f.getExpression().toString());
		}
		return sb.toString();
	}
	
	private static String getTableColumns(RelationalExprSchema t) {
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


