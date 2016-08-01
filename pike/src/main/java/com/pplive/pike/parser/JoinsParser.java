package com.pplive.pike.parser;

import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

class JoinsParser {

	private final PikeSqlParser _sqlParser;
	private final List<Join> _joins;
	
	public JoinsParser(PikeSqlParser sqlParser, List<Join> joins) {
		assert sqlParser != null;
		assert joins != null;
		
		this._sqlParser = sqlParser;
		this._joins = joins;
	}
	
	public RelationalExprOperator parse() {
		throw new UnsupportedOperationException("JOIN is not implemented yet");
		// todo
	}
}
