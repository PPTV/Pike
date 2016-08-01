package com.pplive.pike.parser;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.FunctionExpression;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.TableDataSource;
import com.pplive.pike.util.CollectionUtil;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LateralOp extends RelationalExprOperator {

	private FunctionExpression _udtfExpr;
	public FunctionExpression functionExpr() {
		return this._udtfExpr;
	}

	private final String _tableAlias;
	public String getTableAliases() {
		return this._tableAlias;
	}

    private final ArrayList<String> _columnAliases;
    public List<String> getColumnAliases() {
        return new ArrayList<String>(this._columnAliases);
    }
    public int getColumnAliasCount() {
        return this._columnAliases.size();
    }

	public LateralOp(RelationalExprOperator child, FunctionExpression udtfExpr,
                     String tableAlias, Iterable<String> columnAliases) {
		if (child == null)
			throw new IllegalArgumentException("child cannot be null");
		if (udtfExpr == null)
			throw new IllegalArgumentException("udtfExpr cannot be null");

        this._udtfExpr = udtfExpr;
		this._tableAlias = tableAlias;
        this._columnAliases = CollectionUtil.copyArrayList(columnAliases);

        setOutputSchema(child, this._columnAliases);
		child.setParent(this);
		this._child = child;
	}
	
	@Override
	public Object accept(IRelationalOpVisitor visitor, Object context) {
		return visitor.visit(context, this);
	}

    private void setOutputSchema(RelationalExprOperator child, ArrayList<String> columnAliases) {
        assert child != null;
        assert columnAliases != null;

        RelationalExprSchema childSchema = child.getOutputSchema();
        ArrayList<Column> outputColumns = new ArrayList<Column>(Arrays.asList(childSchema.getColumns()));
        ArrayList<String> needConvertColumns = new ArrayList<String>(0);

        for(Column col : outputColumns) {
            if (child.getOutputSchema().needConvert(col.getName())){
                needConvertColumns.add(col.getName());
            }
        }

        List<Class<?>> udtfFieldTypes = parseUdtfReturnTypes(this._udtfExpr, columnAliases);

        int n = -1;
        for(String colAlias : columnAliases) {
            n += 1;
            Class<?> fieldType;
            if (n < udtfFieldTypes.size()) {
                fieldType = udtfFieldTypes.get(n);
            }
            else {
                fieldType = Object.class;
            }

            CaseIgnoredString colName = new CaseIgnoredString(colAlias);
            Column col = new Column(colName.value(), "", fieldType);
            outputColumns.add(col);
        }

        TableDataSource tableDataSource = child.getOutputSchema().getTableDataSource();
        CaseIgnoredString tableName = InternalTableName.genTableName(child.getOutputSchema().getName());
        this._outputSchema = new RelationalExprSchema(tableDataSource, tableName.value(), "", outputColumns.toArray(new Column[0]), needConvertColumns);
    }

    private static boolean not(boolean expr) { return !expr; }

    private static List<Class<?>> parseUdtfReturnTypes(FunctionExpression udtfExpr, ArrayList<String> columnAliases) {
        // todo. if Class<?> clazz is a generic type,
        // is there any way to get its generic parameter's actual type?
        // that means, if clazz is Iterable and in source code it's Iterable<String>,
        // then here we could know the generic parameter is String.class

        List<Class<?>> types = new ArrayList<Class<?>>();
        Class<?> methodType = udtfExpr.exprType();
        Class<?> rowType = methodType.getComponentType();
        if (rowType == null) {
            if (not(Iterable.class.isAssignableFrom(methodType))) {
                types.add(methodType);
                return types;
            }
            rowType = Object.class;
        }

        Class<?> fieldType = rowType.getComponentType();
        if (fieldType != null) {
            for(int n = 0; n < columnAliases.size(); n += 1) {
                types.add(fieldType);
            }
            return types;
        }

        if (not(Iterable.class.isAssignableFrom(rowType))) {
            types.add(rowType);
            return types;
        }

        for(int n = 0; n < columnAliases.size(); n += 1) {
            types.add(Object.class);
        }
        return types;
    }

	@Override
	public String toExplainString() {
		return this._child.toExplainString() 
				+ String.format("Lateral:%n")
                + String.format("\ttable: %s%n", this._outputSchema.getName())
                + String.format("\ttable generating function: %s%n", this._udtfExpr.toString())
                + String.format("\tgenerating table alias: %s%n", this._tableAlias)
                + String.format("\tgenerating table columns: %s%n", getColumnString());
	}

    private String getColumnString() {
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for(String col : this._columnAliases){
            n += 1;
            if (n > 1) sb.append(", ");
            sb.append(col);
        }
        return sb.toString();
    }
}
