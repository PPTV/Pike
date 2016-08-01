package com.pplive.pike.parser;

import java.util.ArrayList;

import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.util.CollectionUtil;

public class TopOp extends RelationalExprOperator {
	
	private final long _topNumber;
	public long getTopNumber() {
		return this._topNumber;
	}
	
	private final ArrayList<OrderByColumn> _orderByColumns;
	public Iterable<OrderByColumn> getOrderByColumns() {
		return this._orderByColumns;
	}
	
	public int getOrderByColumnCount() {
		return this._orderByColumns.size();
	}

    private ArrayList<ColumnExpression> _topGroupByColumns;
    public Iterable<ColumnExpression> getTopGroupByColumns() {
        return this._topGroupByColumns;
    }
    public void setTopGroupByColumns(Iterable<ColumnExpression> cols) {
        if (cols != null) {
            this._topGroupByColumns = CollectionUtil.copyArrayList(cols);
        }
        else {
            this._topGroupByColumns = null;
        }
    }

    public int getTopGroupByColumnCount() {
        return this._topGroupByColumns.size();
    }

    public ArrayList<OrderByColumn> getOrderByColumnsInGroup() {
        ArrayList<OrderByColumn> cols = new ArrayList<OrderByColumn>(this._orderByColumns.size());
        for(OrderByColumn c : this._orderByColumns) {
            if (not(isTopGroupByColumn(c.columnExpr().getColumnName()))) {
                cols.add(c);
            }
        }
        return cols;
    }

    public ArrayList<String> getOutputColumnsInGroup() {
        ArrayList<String> cols = new ArrayList<String>(this._outputSchema.getColumns().length);
        for(Column c : this._outputSchema.getColumns()) {
            if (not(isTopGroupByColumn(c.getName()))) {
                cols.add(c.getName());
            }
        }
        return cols;
    }

    private static boolean not(boolean expr) { return !expr; }

    private boolean isTopGroupByColumn(String col) {
        for(ColumnExpression expr : this._topGroupByColumns) {
            if (col.equalsIgnoreCase(expr.getColumnName())) {
                return true;
            }
        }
        return false;
    }

    public TopOp(RelationalExprOperator child, long topNumber, Iterable<OrderByColumn> orderByColumns) {
        this(child, topNumber, orderByColumns, null);
    }

    public TopOp(RelationalExprOperator child, long topNumber, Iterable<OrderByColumn> orderByColumns, Iterable<ColumnExpression> topGroupByColumns) {
		if (child == null)
			throw new IllegalArgumentException("child cannot be null");
		if (topNumber < 0)
			throw new IllegalArgumentException("topNumber must be >= 0, 0 means no limit");
		
		this._topNumber = topNumber;
		if (orderByColumns == null){
			this._orderByColumns = new ArrayList<OrderByColumn>(0);
		}
		else{
			this._orderByColumns = CollectionUtil.copyArrayList(orderByColumns);
		}
        if (topGroupByColumns == null){
            this._topGroupByColumns = new ArrayList<ColumnExpression>(0);
        }
        else{
            this._topGroupByColumns = CollectionUtil.copyArrayList(topGroupByColumns);
        }

		this._outputSchema = child.getOutputSchema();
		child.setParent(this);
		this._child = child;
	}

	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toExplainString() {
		return this._child.toExplainString() 
				+ String.format("Top:%n")
				+ String.format("\trow count: %s%n", getTopRowCount(this._topNumber))
                + String.format("\torder by: %s%n", getOrderByColumns(this._orderByColumns))
                + String.format("\ttop group by: %s%n", getTopGroupByColumns(this._topGroupByColumns))
                + String.format("\toutput table: <same as input>%n");
    }
	
	private static String getTopRowCount(long n){
		return n == 0 ? "unlimited" : Long.toString(n);
	}

    private static String getOrderByColumns(ArrayList<OrderByColumn> orderByColumns){
        int n = -1;
        StringBuilder sb = new StringBuilder(100);
        for(OrderByColumn c : orderByColumns){
            n += 1;
            if (n > 0) sb.append(", ");
            sb.append(c);
        }
        return sb.toString();
    }

    private static String getTopGroupByColumns(ArrayList<ColumnExpression> topGroupByColumns){
        int n = -1;
        StringBuilder sb = new StringBuilder(100);
        for(ColumnExpression c : topGroupByColumns){
            n += 1;
            if (n > 0) sb.append(", ");
            sb.append(c);
        }
        return sb.toString();
    }
}
