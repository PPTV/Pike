package com.pplive.pike.parser;

import java.util.*;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.output.OutputField;
import com.pplive.pike.exec.output.OutputTarget;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.expression.ExpressionRefColumnsGetter;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableDataSource;
import com.pplive.pike.util.CollectionUtil;

public class LogicalQueryPlan {

    public static class StreamingColumns {
        private final Table _table;
        public Table table() { return this._table; }

        private final boolean _allColumns;
        public boolean allColumns() { return this._allColumns; }

        private final ArrayList<String> _requiredColumns;
        public ArrayList<String> requiredColumns() { return this._requiredColumns; }

        public StreamingColumns(Table table, Iterable<String> requiredColumns, boolean allColumns) {
            assert table.getTableDataSource() == TableDataSource.Streaming;

            this._table = table;
            this._requiredColumns = CollectionUtil.copyArrayList(requiredColumns);
            this._allColumns = allColumns;
        }
    }

	private final RelationalExprOperator _rootOp;
	
	private final HashMap<String, Object> _pikeOrStormOptions;
	public Map<String, Object> getParsedOptions() {
		return new HashMap<String, Object>(this._pikeOrStormOptions);
	}
	
	private final Period _baseProcessPeriod;
	public Period getBaseProcessPeriod() {
		return this._baseProcessPeriod;
	}
	
	private final ArrayList<TransformField> _outputItems; // the OUTPUT(...) items
	public Iterable<TransformField> getOutputItems(){
		return this._outputItems;
	}
	public int getOutputItemCount(){
		return this._outputItems != null ? this._outputItems.size() : 0;
	}
		
	private ArrayList<CaseIgnoredString> _inputOrderSelectItems;
	public ArrayList<CaseIgnoredString> getItemsInputOrder(){
		assert this._inputOrderSelectItems != null;
		return new ArrayList<CaseIgnoredString>(this._inputOrderSelectItems);
	}
	
	private ArrayList<OutputTarget> _outputTargets;
	public Iterable<OutputTarget> getOutputTargets() {
		return this._outputTargets;
	}
	public int getOutputTargetCount() {
		return this._outputTargets != null ? this._outputTargets.size() : 0;
	}

	public LogicalQueryPlan(Map<String, Object> pikeOrStormOptions, Period baseOutputPeriod,
			RelationalExprOperator rootOp, Iterable<CaseIgnoredString> inputOrderSelectItems,
			Iterable<TransformField> outputItems, Iterable<OutputTarget> outputTargets) {
		if (pikeOrStormOptions == null)
			throw new IllegalArgumentException("pikeOrStormOptions cannot be null");
		if (baseOutputPeriod == null)
			throw new IllegalArgumentException("defaultOutputPeriod cannot be null");
		if (rootOp == null)
			throw new IllegalArgumentException("rootOp cannot be null");
		if (inputOrderSelectItems == null)
			throw new IllegalArgumentException("inputOrderSelectItems cannot be null");
		if (outputItems == null)
			throw new IllegalArgumentException("outputItems cannot be null");
		if (outputTargets == null)
			throw new IllegalArgumentException("outputTargets cannot be null");
		
		this._pikeOrStormOptions = new HashMap<String, Object>(pikeOrStormOptions);
		this._baseProcessPeriod = baseOutputPeriod;
		this._rootOp = rootOp;
		this._inputOrderSelectItems = CollectionUtil.copyArrayList(inputOrderSelectItems);
		this._outputItems = CollectionUtil.copyArrayList(outputItems);
		this._outputTargets = CollectionUtil.copyArrayList(outputTargets);
	}
	
	public RelationalExprOperator getRootOp() {
		return this._rootOp;
	}

    public StreamingColumns getStreamingTableRequiredColumns() {
        LeafTableOp leafTableOp = (LeafTableOp)getLeafOp();
        Table table = leafTableOp.getOutputSchema();
        ArrayList<String> cols = new ArrayList<String>();

        if (leafTableOp.getParent() != null
                && leafTableOp.getParent().getClass() == ProjectOp.class){
            ProjectOp projectOp = (ProjectOp)leafTableOp.getParent();

            for(Column col : projectOp.getProjectColumns()) {
                if (table.getColumn(col.getName()) == null) {
                   assert false;
                   String msg = String.format("bug: program run incorrectly, should never happen: table %s has no column %s", table.getName(), col.getName());
                   throw new IllegalStateException(msg);
                }
                cols.add(col.getName());
            }
            return new StreamingColumns(table, cols, false);
        }
        else {
            for(Column col : table.getColumns()){
                cols.add(col.getName());
            }
            return new StreamingColumns(table, cols, true);
        }
    }

    public RelationalExprOperator getLeafOp() {
        RelationalExprOperator op = getRootOp();
        while(op.getChild() != null){
            assert op.getChildren().length == 1;
            op = op.getChild();
        }
        assert op.getClass() == LeafTableOp.class;
        return op;
    }

	public ArrayList<OutputField> getOutputFields() {
		
		final ArrayList<CaseIgnoredString> inputOrderSelectItems = getItemsInputOrder();
		final ArrayList<OutputField> outputFields = new ArrayList<OutputField>(inputOrderSelectItems.size());
		for(CaseIgnoredString alias : inputOrderSelectItems){
			TransformField field = findInOutputItems(alias);
			if (field != null){
				OutputField f = new OutputField(field.getAlias().value(), field.getExpression());
				outputFields.add(f);
				continue;
			}
			Column col = findInOutputSchema(alias);
			if (col != null){
				OutputField f = new OutputField(col.getName(), col.getColumnType());
				outputFields.add(f);
			}
			else{
				assert false;
				throw new IllegalStateException("should never happen: final output select item not found");
			}
		}
		
		return outputFields;
	}
	
	private TransformField findInOutputItems(CaseIgnoredString alias) {
		assert alias != null;
		for(TransformField field : getOutputItems()){
			if (field.getAlias().equals(alias))
				return field;
		}
		return null;
	}

	private Column findInOutputSchema(CaseIgnoredString alias) {
		assert alias != null;
		final Table outputTable = getRootOp().getOutputSchema();
		for(Column c : outputTable.getColumns()){
			if (alias.equalsString(c.getName()))
				return c;
		}
		return null;
	}
	
	public void optimize() {
		optimize1();
	}
	
	private void optimize1() {
		// naive strategy: 
		// change LeafTableOp[->RenameOp or ->SelectOp]->ProjectOp
		//     to LeafTableOp->ProjectOp[->RenameOp or ->SelectOp][->ProjectOp]
		// TODO: to improve in future
		
		RelationalExprOperator op = this._rootOp;
		while(op.getChild() != null){
			op = op.getChild();
		}
		assert op instanceof LeafTableOp;
		op = op.getParent();
		while(op != null && (op instanceof RenameOp || op instanceof SelectOp)){
			op = op.getParent();
		}
		if ((op instanceof ProjectOp) == false)
			return;
		
		ProjectOp projectOp = (ProjectOp)op;
		
		if (projectOp.getChild() instanceof SelectOp) {
			SelectOp filterOp = (SelectOp) op.getChild();

			HashMap<CaseIgnoredString, Column> cols = new HashMap<CaseIgnoredString, Column>();
			for (Column col : projectOp.getProjectColumns()) {
				cols.put(new CaseIgnoredString(col.getName()), col);
			}
			int columnCount = cols.size();
			cols.putAll(ExpressionRefColumnsGetter.getAllReferencedColumns(filterOp.conditionExpr()));

			if (cols.size() == columnCount) {
				// move ProjectOp
				filterOp.setParent(projectOp.getParent());
				projectOp.setChild(filterOp.getChild());
				filterOp.setChild(projectOp);
				projectOp.setParent(filterOp);
				projectOp.getChild().setParent(projectOp);
				if (filterOp.getParent() != null) {
					filterOp.getParent().setChild(filterOp);
				}
			} else {
				// create new ProjectOp
				ArrayList<ProjectField> fields = new ArrayList<ProjectField>(
						cols.size());
				for (Column col : cols.values()) {
					ColumnExpression expr = new ColumnExpression(col);
					fields.add(new ProjectField(expr));
				}
				projectOp = new ProjectOp(filterOp.getChild(), fields);
				projectOp.setParent(filterOp);
				filterOp.setChild(projectOp);
				projectOp.getChild().setParent(projectOp);
			}
		}
		
		if (projectOp.getChild() instanceof RenameOp){
			RenameOp renameOp = (RenameOp)projectOp.getChild();
			RelationalExprOperator parentOp = projectOp.getParent();
			RenameOp newRenameOp = new RenameOp(projectOp, renameOp.getAlias());
			
			newRenameOp.setParent(parentOp);
			if (parentOp != null) {
				parentOp.setChild(newRenameOp);
			}
			projectOp.setChild(renameOp.getChild());
			projectOp.getChild().setParent(projectOp);
		}
		
	}
	
	public String toExplainString() {
		return "Logical Query Plan:\r\n"
                + String.format("Base Process Period: %s%n", this._baseProcessPeriod)
                + String.format("Streaming Table Required Columns: %s%n", requiredColumnsString())
				+ this._rootOp.toExplainString();
	}

    private String requiredColumnsString() {
        StreamingColumns cols = this.getStreamingTableRequiredColumns();
        String s = String.format("[all columns:%s] ", cols.allColumns() ? "yes" : "no");
        for(String col : cols.requiredColumns()){
            s += col + ", ";
        }
        return s;
    }
}
