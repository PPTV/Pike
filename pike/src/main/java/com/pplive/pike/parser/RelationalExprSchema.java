package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.Arrays;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.Immutable;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableDataSource;
import com.pplive.pike.util.CollectionUtil;

// this class is designed due to historical reason, it's not necessary currently.
// it holds a list of columns that must add a type conversion operation for
// those tuple fields being their types correctly as declared in schema.
// this is originally because we add some aggregation functions in trident (e.g. count_distinct, topN, etc),
// however trident doesn't support aggregate state data type being different from final result data type.
// also see comments in ExpressionParser.visit(net.sf.jsqlparser.schema.Column tableColumn)
@Immutable
public class RelationalExprSchema extends Table {

	private final ArrayList<String> _columnsNeedConvert;
	public Iterable<String> getColumnsNeedConvert() {
		return this._columnsNeedConvert;
	}
	
	RelationalExprSchema(Table source){
		this(source.getTableDataSource(), source.getName(), source.getTitle(), source.getColumns(), Arrays.asList(new String[0]));
	}
	
	RelationalExprSchema(TableDataSource tableDataSource, String name, String title, Column[] columns, Iterable<String> columnsNeedConvert){
		super(tableDataSource, name, title, columns);
		if (columnsNeedConvert == null){
			throw new IllegalArgumentException("columnsNeedConvert cannot be null");
		}
		this._columnsNeedConvert = CollectionUtil.copyArrayList(columnsNeedConvert);
	}
	
	public boolean needConvert(String column){
		return needConvert(new CaseIgnoredString(column));
	}
	
	public boolean needConvert(CaseIgnoredString column){
		for(String s : this._columnsNeedConvert){
			if (column.equalsString(s))
				return true;
		}
		return false;
	}
}
