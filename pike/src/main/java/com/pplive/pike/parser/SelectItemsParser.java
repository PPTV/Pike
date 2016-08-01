package com.pplive.pike.parser;

import java.util.*;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.AggregateExpression;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.expression.ExpressionAggregateCallInspector;
import com.pplive.pike.expression.ExpressionRefColumnsGetter;
import com.pplive.pike.expression.OutputExpression;
import com.pplive.pike.metadata.Column;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

class SelectItemsParser implements SelectItemVisitor {

	private final PikeSqlParser _sqlParser;
	private final RelationalExprOperator _childOp;
	private final SchemaScope _schemaScope;
	private final List<SelectItem> _selectItems;
	private final ArrayList<TableColumn> _groupByCols;
	
	private ArrayList<TransformField> _outputItems; // the OUTPUT(...) items, cannot reference any column, cannot be in group by.
	public Iterable<TransformField> getOutputItems(){
		return this._outputItems;
	}
	public int getOutputItemCount(){
		return this._outputItems != null ? this._outputItems.size() : 0;
	}
	
	private ArrayList<TransformField> _nonAggregateSelectItems;
	private ArrayList<TransformField> _aggregateSelectItems;
	private ArrayList<Column> _groupByColumns;
	
	private ArrayList<CaseIgnoredString> _inputOrderSelectItems;
	public ArrayList<CaseIgnoredString> getItemsInputOrder(){
		assert this._inputOrderSelectItems != null;
		return new ArrayList<CaseIgnoredString>(this._inputOrderSelectItems);
	}
	
	private ArrayList<Exception> _parseErrors = new ArrayList<Exception>();
	private void addError(Exception e){
		this._parseErrors.add(e);
	}

	public SelectItemsParser(PikeSqlParser sqlParser, RelationalExprOperator childOp, SchemaScope schemaScope,
			List<SelectItem> selectItems, List<TableColumn> groupByCols) {
		assert sqlParser != null;
		assert childOp != null;
		assert selectItems != null;
		assert groupByCols != null;
		
		this._sqlParser = sqlParser;
		this._childOp = childOp;
		this._schemaScope = schemaScope != null ? schemaScope : new SchemaScope(childOp.getOutputSchema());
		this._selectItems = selectItems;
		this._groupByCols = new ArrayList<TableColumn>();
		this._groupByCols.addAll(groupByCols);
	}
	
	public RelationalExprOperator parse() {
		this._inputOrderSelectItems = new ArrayList<CaseIgnoredString>(this._selectItems.size());
		this._outputItems = new ArrayList<TransformField>();
		this._nonAggregateSelectItems = new ArrayList<TransformField>();
		this._aggregateSelectItems = new ArrayList<TransformField>();
		this._groupByColumns = new ArrayList<Column>(this._groupByCols.size());
		for(SelectItem item : this._selectItems) {
			item.accept(this);
		}
		
		checkDuplicateColumnNames();
		checkAggregateSemantics();
		
		if (this._parseErrors.size() > 0){
			throw new ParseErrorsException(this._parseErrors);
		}
		
		RelationalExprOperator childOp = this._childOp;
		List<ProjectField> fields = getAllReferencedColumns(this._nonAggregateSelectItems, this._groupByColumns, this._aggregateSelectItems);
		if (fields.size() < childOp.getOutputSchema().getColumns().length){
            if (fields.size() == 0) {
                // it's possible that a SELECT clause contains no column reference.
                // we MUST choose a column, otherwise no tuple generated in corresponding bolt, and its destination bolts has no input data.
                Column col = chooseColumnForEmptyProject(childOp.getOutputSchema());
                ColumnExpression colExpr = new ColumnExpression(col);
                ProjectField projectField = new ProjectField(colExpr);
                fields = Arrays.asList(projectField);
            }
			ProjectOp projectOp = new ProjectOp(childOp, fields);
			childOp = projectOp;
		}

		if (this._aggregateSelectItems.size() > 0) {
			return AggregateOp.create(childOp, this._nonAggregateSelectItems, this._groupByColumns, this._aggregateSelectItems);
		}
		else{
			if (ProjectOp.isOnlyProjection(this._nonAggregateSelectItems)) {
				return childOp;
			}
			else {
				return new TransformOp(childOp, this._nonAggregateSelectItems);
			}
		}
	}

    private static Column chooseColumnForEmptyProject(RelationalExprSchema table) {
        // try choose a column that can save network bandwidth.
        Column[] cols = table.getColumns();
        for(Column c : cols){
            Class<?> t  = ColumnType.convert(c.getColumnType());
            if (t == Boolean.class || Number.class.isAssignableFrom(t)
                    || t == java.sql.Date.class || t == java.sql.Time.class || t == java.sql.Timestamp.class )
                return c;
        }
        for(Column c : cols){
            if (c.getColumnType() == ColumnType.String)
                return c;
        }
        return cols[0];
    }

	private static ArrayList<ProjectField> getAllReferencedColumns(Iterable<TransformField> nonAggregateItems, Iterable<Column> groupByColumns, Iterable<TransformField> aggregateItems) {
		HashMap<CaseIgnoredString, Column> columns = new HashMap<CaseIgnoredString, Column>();
		HashSet<CaseIgnoredString> nonAggregateAliases = new HashSet<CaseIgnoredString>();
		for(TransformField f : nonAggregateItems) {
			Map<CaseIgnoredString, Column> cols = ExpressionRefColumnsGetter.getAllReferencedColumns(f.getExpression());
			columns.putAll(cols);
			nonAggregateAliases.add(f.getAlias());
		}
		for(Column col : groupByColumns) {
			CaseIgnoredString colName = new CaseIgnoredString(col.getName());
			if (nonAggregateAliases.contains(colName) == false)
				columns.put(colName, col);
		}
		for(TransformField f : aggregateItems) {
			Map<CaseIgnoredString, Column> cols = ExpressionRefColumnsGetter.getAllReferencedColumns(f.getExpression());
			columns.putAll(cols);
		}
		
		ArrayList<ProjectField> result = new ArrayList<ProjectField>();
		for(Column col : columns.values()){
			ColumnExpression expr = new ColumnExpression(col);
			result.add(new ProjectField(expr));
		}
		return result;
	}
	
	private void checkDuplicateColumnNames() {
		HashMap<CaseIgnoredString, Integer> cols = new HashMap<CaseIgnoredString, Integer>();
		for(TransformField f : this._nonAggregateSelectItems){
			CaseIgnoredString k = f.getAlias();
			if (cols.containsKey(k)){
				cols.put(k, cols.get(k) + 1);
			}
			else{
				cols.put(k, 1);
			}
		}
		for(TransformField f : this._aggregateSelectItems){
			CaseIgnoredString k = f.getAlias();
			if (cols.containsKey(k)){
				cols.put(k, cols.get(k) + 1);
			}
			else{
				cols.put(k, 1);
			}
		}
		for(Map.Entry<CaseIgnoredString, Integer> item : cols.entrySet()){
			if (item.getValue() > 1){
				String msg = String.format("Duplicate alias: '%s' appear %d times", item.getKey(), item.getValue());
				addError(new SemanticErrorException(msg));
			}
		}
	}
	
	private void checkAggregateSemantics() {
		if (this._aggregateSelectItems.size() == 0) {
			if (this._groupByCols.size() > 0){
				addError(new SemanticErrorException("GROUP BY error: there is no aggregation in SELECT items"));
			}
			return;
		}
		
		for(TransformField f : this._nonAggregateSelectItems){
			if (containInGroupBy(f, this._groupByCols) == false){
				String msg = String.format("GROUP BY error: non-aggregate SELECT item %s is not in GROUP BY columns", f.getAlias());
				addError(new SemanticErrorException(msg));
			}
		}
		
		this._groupByColumns = new ArrayList<Column>(this._groupByCols.size());
		
		for(int n = 0; n < this._groupByCols.size(); n +=1){
			TableColumn col = this._groupByCols.get(n);
			TransformField f = findInNonAggregateItems(this._nonAggregateSelectItems, col);
			if (f != null){
				if (ColumnType.isSimpleValueType(f.getExpression().exprType())){
					this._groupByColumns.add(new Column(f.getAlias().value(), "", f.getExpression().exprType()));
				}
				else {
					String msg = String.format("GROUP BY error: %s is not simple type, cannot be in GROUP BY columns", f.getAlias());
					addError(new SemanticErrorException(msg));
				}
				continue;
			}
			net.sf.jsqlparser.schema.Table tableExpr = new net.sf.jsqlparser.schema.Table(null, null);
			if(col.tableName().isEmpty() == false){
				tableExpr.setName(col.tableName().value());
			}
			net.sf.jsqlparser.schema.Column columnExpr = new net.sf.jsqlparser.schema.Column(tableExpr, col.columnName().value());
			ExpressionParser parser = new ExpressionParser(this._sqlParser, this._schemaScope, columnExpr);
			try {
				AbstractExpression expr = parser.parse();
				assert expr.getClass() == ColumnExpression.class;
				ColumnExpression colExpr = (ColumnExpression)expr;
				if (ColumnType.isSimpleValueType(colExpr.exprType())){
					this._groupByColumns.add(new Column(colExpr.getColumnName(), "", colExpr.exprType()));
				}
				else {
					String msg = String.format("GROUP BY error: %s is not simple type, cannot be in GROUP BY columns", colExpr.getColumnName());
					addError(new SemanticErrorException(msg));
				}
			}
			catch(ParseErrorsException e) {
				for(Exception err : e.getParseErrors())
					addError(err);
				return;
			}
		}
	}
	
	private static boolean containInGroupBy(TransformField nonAggregateSelectItem, ArrayList<TableColumn> groupByCols){
		for(TableColumn col : groupByCols){
			if (nonAggregateSelectItem.getAlias().equals(col.columnName()))
				return true;
		}
		return false;
	}
	
	private static TransformField findInNonAggregateItems(ArrayList<TransformField> nonAggregateSelectItems, TableColumn col){
		for(TransformField f : nonAggregateSelectItems){
			if (f.getAlias().equals(col.columnName()))
				return f;
		}
		return null;
	}

	public void visit(AllColumns allColumns) {
		addError(new UnsupportedOperationException("SELECT * is not implemented yet."));
	}
	
	public void visit(AllTableColumns allTableColumns) {
		addError(new UnsupportedOperationException("SELECT <table>.* is not implemented yet."));
	}
	
	public void visit(SelectExpressionItem selectExpressionItem) {
		Alias alias = selectExpressionItem.getAlias();
		Expression expr = selectExpressionItem.getExpression();
		ExpressionParser parser = new ExpressionParser(this._sqlParser, this._schemaScope, expr);
		AbstractExpression parsedExpr = null;
		try {
			parsedExpr = parser.parse();
		}
		catch(ParseErrorsException e){
			for(Exception err : e.getParseErrors())
				addError(err);
			return;
		}
		TransformField transformField;
		if (alias != null) {
			assert alias.getName().isEmpty() == false;
			transformField = new TransformField(parsedExpr, alias.getName());
		}
		else {
			transformField = new TransformField(parsedExpr);
		}
		
		this._inputOrderSelectItems.add(transformField.getAlias());
		
		if (parsedExpr instanceof OutputExpression){
			this._outputItems.add(transformField);
		}
		else if (ExpressionAggregateCallInspector.containsAggregateCall(transformField.getExpression())){
			this._aggregateSelectItems.add(transformField);
		}
		else{
			this._nonAggregateSelectItems.add(transformField);
		}
	}
	
}
