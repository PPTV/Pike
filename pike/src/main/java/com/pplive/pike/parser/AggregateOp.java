package com.pplive.pike.parser;

import java.util.*;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.*;
import com.pplive.pike.function.builtin.BuiltinFunctions;
import com.pplive.pike.function.builtin.IBuiltinFunctionParser;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableDataSource;
import com.pplive.pike.util.CollectionUtil;

public class AggregateOp extends TransformOp {
	
	public boolean hasNonAggregateFields() {
		return this._nonAggregateFields.size() > 0 || this._columnsInGroupBy.size() > 0;
	}

	private ArrayList<TransformField> _aggregateFields;
	public Iterable<TransformField> getAggregateFields() {
		return this._aggregateFields;
	}
	public int getAggregateFieldCount() {
		return this._aggregateFields.size();
	}
	
	private ArrayList<Column> _groupByColumns;
	
	private HashMap<CaseIgnoredString, Column> _columnsInNonAggregateItems;
	public Iterable<Column> getColumnsInNonAggregateItems() {
		return this._columnsInNonAggregateItems.values();
	}
	
	private HashMap<CaseIgnoredString, Column> _columnsInAggregateItems;
	public Iterable<Column> getColumnsInAggregateItems() {
		return this._columnsInAggregateItems.values();
	}
	
	private HashMap<CaseIgnoredString, Column> _columnsInGroupBy; // exclude aliases in _nonAggregateFields;
	public Iterable<Column> getColumnsInGroupBy() {
		return this._columnsInGroupBy.values();
	}

    private static boolean isAggregateCall(AbstractExpression expr) {
        return expr instanceof  AggregateExpression;
    }

    public static TransformOp create(RelationalExprOperator child, Iterable<TransformField> nonAggregateFields,
										Iterable<Column> groupByColumns, Iterable<TransformField> aggregateFields) {
		AggregateOp aggregateOp = new AggregateOp(child, nonAggregateFields, groupByColumns, aggregateFields);

        boolean needTransform = false;
        for(TransformField f : aggregateOp._aggregateFields) {
            if (not( isAggregateCall(f.getExpression()) )) {
                needTransform = true;
                break;
            }
        }
        if (not(needTransform)) {
            return aggregateOp;
        }

        ArrayList<TransformField> transformFields = new ArrayList<TransformField>(aggregateOp._aggregateFields.size());
        ArrayList<TransformField> newAggregateFields = new ArrayList<TransformField>(transformFields.size());
        for(TransformField f : aggregateOp._aggregateFields) {
            if ( isAggregateCall(f.getExpression()) ) {
                newAggregateFields.add(f);

                ColumnType colType = ColumnType.convert(f.getExpression().exprType());
                ColumnExpression colExpr = new ColumnExpression(colType, f.getAlias().value());
                transformFields.add(new TransformField(colExpr, f.getAlias()));
            }
            else {
                List<TransformField> aggFields = ExpressionAggregateCallExtractor.extract(f.getExpression());
                newAggregateFields.addAll(aggFields);

                transformFields.add(f);
            }
        }

        aggregateOp._aggregateFields = newAggregateFields;
        aggregateOp.setOutputSchema(aggregateOp._child, aggregateOp._nonAggregateFields, newAggregateFields);

        ArrayList<TransformField> fields = CollectionUtil.copyArrayList(aggregateOp._nonAggregateFields);
        fields.addAll(transformFields);
        TransformOp transformOp = new TransformOp(aggregateOp, fields);
        return transformOp;
	}

    private static boolean not(boolean expr) { return !expr; }
	
	private AggregateOp(RelationalExprOperator child, Iterable<TransformField> nonAggregateFields, 
						Iterable<Column> groupByColumns, Iterable<TransformField> aggregateFields) {
		super(child, nonAggregateFields);

		if (groupByColumns == null)
			throw new IllegalArgumentException("groupByColumns cannot be null");
		if (aggregateFields == null)
			throw new IllegalArgumentException("aggregateFields cannot be null");
		
		assert(CollectionUtil.sizeOf(groupByColumns) >= CollectionUtil.sizeOf(nonAggregateFields));
		
		this._groupByColumns = CollectionUtil.copyArrayList(groupByColumns);
		
		if (not( allAreColumnExpressionAndNoAlias(nonAggregateFields) )){
			// we need split to AggregateOp on top of a TransformOp,
			// make non-aggregate items in AggregateOp are all simple ColumnExpressions 

			setProjectColumns(nonAggregateFields, groupByColumns, aggregateFields);
			
			ArrayList<TransformField> tranformFields = new ArrayList<TransformField>();
			for(TransformField f : nonAggregateFields) {
				if (f.getExpression().getClass() == ColumnExpression.class){
					tranformFields.add(new TransformField(f.getExpression(), f.getAlias()));
				}
				else{
					tranformFields.add(f);
				}
			}
			for(Column col : this._columnsInGroupBy.values()) {
				if (containsColumnExpression(tranformFields, col) == false){
					ColumnExpression expr = new ColumnExpression(col);
					tranformFields.add(new TransformField(expr));
				}
			}
			for(Column col : this._columnsInAggregateItems.values()) {
				if (containsColumnExpression(tranformFields, col) == false){
					ColumnExpression expr = new ColumnExpression(col);
					tranformFields.add(new TransformField(expr));
				}
			}
			
			TransformOp transformOp = new TransformOp(child, tranformFields);
			this._child = transformOp;
			transformOp.setParent(this);
			
			this._projectColumns = new HashMap<CaseIgnoredString, Column>();
			this._nonAggregateFields = new ArrayList<TransformField>(10);
			for(TransformField f : nonAggregateFields){
				final AbstractExpression expr = f.getExpression();
				final ColumnExpression columnExpr = new ColumnExpression("", ColumnType.convert(expr.exprType()), f.getAlias().value());
				this._nonAggregateFields.add(new ProjectField(columnExpr));
				Column col = new Column(columnExpr.getColumnName(), "", columnExpr.exprType());
				this._projectColumns.put(new CaseIgnoredString(col.getName()), col);
			}
			
			for(Column col : groupByColumns) {
				if (containsAlias(nonAggregateFields, col.getName()) == false)
					this._projectColumns.put(new CaseIgnoredString(col.getName()), col);
			}
		}
		
		this._aggregateFields = CollectionUtil.copyArrayList(aggregateFields);

		for(TransformField f : aggregateFields) {
			Map<CaseIgnoredString, Column> cols = ExpressionRefColumnsGetter.getAllReferencedColumns(f.getExpression());
			assert cols != null;
			this._projectColumns.putAll(cols);
		}
		
		setProjectColumns(this._nonAggregateFields, groupByColumns, aggregateFields);
		setOutputSchema(this._child, this._nonAggregateFields, aggregateFields);
	}
	
	private static boolean allAreColumnExpressionAndNoAlias(Iterable<TransformField> nonAggregateFields) {
		for(TransformField f : nonAggregateFields) {
			if (f.getExpression().getClass() != ColumnExpression.class)
				return false;
			ColumnExpression expr = (ColumnExpression)f.getExpression();
			if (f.getAlias().value().equals(expr.getColumnName()) == false) // strict compare
				return false;
		}
		return true;
	}
	
	private static boolean containsColumnExpression(Iterable<TransformField> fields, Column col) {
		for(TransformField f : fields){
			AbstractExpression expr = f.getExpression();
			if (expr.getClass() == ColumnExpression.class
					&& col.getName().equalsIgnoreCase( ((ColumnExpression)expr).getColumnName() )){
				return true;
			}
		}
		return false;
	}
	
	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}

	
	private void setProjectColumns(Iterable<TransformField> nonAggregateFields, Iterable<Column> groupByColumns, Iterable<TransformField> aggregateFields) {
		assert nonAggregateFields != null;
		assert aggregateFields != null;
		
		this._columnsInNonAggregateItems = new HashMap<CaseIgnoredString, Column>();
		for(TransformField f : nonAggregateFields) {
			Map<CaseIgnoredString, Column> cols = ExpressionRefColumnsGetter.getAllReferencedColumns(f.getExpression());
			assert cols != null;
			this._columnsInNonAggregateItems.putAll(cols);
		}
		
		this._columnsInGroupBy = new HashMap<CaseIgnoredString, Column>();
		for(Column col : groupByColumns) {
			if (containsAlias(nonAggregateFields, col.getName()) == false)
				this._columnsInGroupBy.put(new CaseIgnoredString(col.getName()), col);
		}

		this._columnsInAggregateItems = new HashMap<CaseIgnoredString, Column>();
		for(TransformField f : aggregateFields) {
			Map<CaseIgnoredString, Column> cols = ExpressionRefColumnsGetter.getAllReferencedColumns(f.getExpression());
			assert cols != null;
			this._columnsInAggregateItems.putAll(cols);
		}
	}
	
	private static boolean containsAlias(Iterable<TransformField> nonAggregateFields, String alias){
		for(TransformField f : nonAggregateFields){
			if (f.getAlias().equalsString(alias))
				return true;
		}
		return false;
	}
	
	private void setOutputSchema(RelationalExprOperator child, Iterable<TransformField> nonAggregateFields, Iterable<TransformField> aggregateFields) {
		assert child != null;
		assert nonAggregateFields != null;
		assert aggregateFields != null;
		
		ArrayList<Column> outputColumns = new ArrayList<Column>();
		ArrayList<String> needConvertAggregateColumns = new ArrayList<String>();
		for(TransformField f : nonAggregateFields) {
			CaseIgnoredString colName = f.getAlias();
			Column col = new Column(colName.value(), "", f.getExpression().exprType());
			outputColumns.add(col);
			if (f.getExpression().needConvertResultToExprType()){
				needConvertAggregateColumns.add(colName.value());
			}
		}
		for(TransformField f : aggregateFields) {
			CaseIgnoredString colName = f.getAlias();
			Column col = new Column(colName.value(), "", f.getExpression().exprType());
			outputColumns.add(col);
			if (f.getExpression().needConvertResultToExprType()){
				needConvertAggregateColumns.add(colName.value());
			}
		}
		
		TableDataSource tableDataSource = child.getOutputSchema().getTableDataSource();
		CaseIgnoredString tableName = InternalTableName.genTableName(child.getOutputSchema().getName());
		this._outputSchema = new RelationalExprSchema(tableDataSource, tableName.value(), "", outputColumns.toArray(new Column[0]), needConvertAggregateColumns);
	}
	
	@Override
	public String toExplainString() {
		return this._child.toExplainString() 
				+ String.format("Aggregate:%n")
				+ String.format("\tinput table: %s%n", this._child.getOutputSchema().getName())
				+ String.format("\tproject columns: %s%n",  getColumnsString(this._projectColumns.values()))
				+ String.format("\tnon-aggregate items: %s%n", getTransformFieldsExprString(this._nonAggregateFields))
				+ String.format("\taggregate items: %s%n", getTransformFieldsExprString(this._aggregateFields))
				+ String.format("\tgroup by items: %s%n", getColumnsString(this._groupByColumns))
				+ String.format("\toutput table: %s(%s)%n", this._outputSchema.getName(), getTableColumns(this._outputSchema))
				;
	}
	
}
