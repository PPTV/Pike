package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.List;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.exec.output.OutputTarget;
import com.pplive.pike.exec.output.OutputType;
import com.pplive.pike.expression.ColumnExpression;
import com.pplive.pike.util.CollectionUtil;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;

class SelectParser implements SelectVisitor {

	private final PikeSqlParser _sqlParser;
	private final SchemaScope _schemaScope;
	private final SelectBody _selectBody;
	private final boolean _subQuery;
	
	private ArrayList<TransformField> _outputItems; // the OUTPUT(...) items, cannot reference any column, cannot be in group by.
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

	private ArrayList<Exception> _parseErrors = new ArrayList<Exception>();
	private void addError(Exception e){
		this._parseErrors.add(e);
	}
	
	public SelectParser(PikeSqlParser sqlParser, SchemaScope schemaScope, SelectBody selectBody) {
		this(sqlParser, schemaScope, selectBody, false);
	}
	
	public SelectParser(PikeSqlParser sqlParser, SchemaScope schemaScope, SelectBody selectBody, boolean subQuery) {
		assert sqlParser != null;
		assert selectBody != null;
		
		this._sqlParser = sqlParser;
		this._schemaScope = schemaScope;
		this._selectBody = selectBody;
		this._subQuery = subQuery;
	}
	
	private RelationalExprOperator _unionRootOp;
	private RelationalExprOperator _oneSelectRootOp;

	private ArrayList<OutputTarget> _outputTargets;
	public Iterable<OutputTarget> getOutputTargets() {
		return this._outputTargets;
	}
	public int getOutputTargetCount() {
		return this._outputTargets != null ? this._outputTargets.size() : 0;
	}
	
	public RelationalExprOperator parse() {
		this._unionRootOp = null;
		this._oneSelectRootOp = null;
		
		this._selectBody.accept(this);
		if (this._parseErrors.size() > 0){
			throw new ParseErrorsException(this._parseErrors);
		}
		
		if (this._unionRootOp != null) {
			return this._unionRootOp;
		}
		else {
			assert this._oneSelectRootOp != null;
			return this._oneSelectRootOp;
		}
	}
	
	// visit methods of SelectVisitor
	public void visit(PlainSelect plainSelect) {
		this._oneSelectRootOp = null;

		// parse intoTable 
		@SuppressWarnings("unchecked") List<net.sf.jsqlparser.statement.select.IntoTarget> intoTargets = plainSelect.getInto();
		if (intoTargets == null) {
			this._outputTargets = new ArrayList<OutputTarget>(0);
		}
		else{
			this._outputTargets = new ArrayList<OutputTarget>(intoTargets.size());
			for(net.sf.jsqlparser.statement.select.IntoTarget target : intoTargets){
                net.sf.jsqlparser.schema.Table t = target.getTarget();
				if (t.getSchemaName() == null || t.getSchemaName().isEmpty()){
					addError(new SemanticErrorException("INTO target must specify output type, e.g. Jdbc.MyTable. available types: Console, SocketServer, File, Jdbc, HBase, SQLServerBulk, Hdfs"));
					continue;
				}
				OutputType outputType = OutputType.parse(t.getSchemaName());
				if (outputType == OutputType.Unknown){
					addError(new SemanticErrorException("INTO target output type is unknown, available types: Console, SocketServer, File, Jdbc, HBase, SQLServerBulk, Hdfs"));
					continue;
				}
				if (outputType == OutputType.Console){
					if (t.getName().equalsIgnoreCase("local") == false && t.getName().equalsIgnoreCase("submitter") == false){
						addError(new SemanticErrorException("console output target name can only be one of: local, submitter"));
						continue;
					}
				}
                Period period = target.getOutputPeriod();
                com.pplive.pike.base.Period baseOutputPeriod = this._sqlParser.getProcessPeriod();
                assert baseOutputPeriod != null;
                com.pplive.pike.base.Period outputPeriod;
                if (period != null) {
                    outputPeriod = com.pplive.pike.base.Period.parse(period.getPeriodString());
                    if (outputPeriod.periodSeconds() < baseOutputPeriod.periodSeconds()
                            || outputPeriod.periodSeconds() % baseOutputPeriod.periodSeconds()  != 0) {
                        String msg = String.format(" ... EVERY %s ... : output period must be N times of base process period in SQL begin 'withperiod ...' (%s seconds), N >= 1.",
                                period.getPeriodString(), baseOutputPeriod.periodSeconds());
                        addError(new SemanticErrorException(msg));
                        continue;
                    }
                }
                else {
                    outputPeriod = baseOutputPeriod;
                }
				this._outputTargets.add(new OutputTarget(outputType, t.getName(), outputPeriod));
			}
		}
		
		RelationalExprOperator plainSelectRootOp = null;
		RelationalExprOperator fromOp = null;
		
		// parse fromItem
		FromItem fromItem = plainSelect.getFromItem();
		assert fromItem != null;
		FromItemParser fromItemParser = new FromItemParser(this._sqlParser, fromItem);
		try{
			fromOp = fromItemParser.parse();
			plainSelectRootOp = fromOp;
		}
		catch(ParseErrorsException e){
			for(Exception err : e.getParseErrors())
				addError(err);
			return;
		}
		
		// parse joins
		@SuppressWarnings("unchecked") List<Join> joins = plainSelect.getJoins();
		if (joins != null) {
			//JoinsParser parser = new JoinsParser(this._sqlParser, joins);
			//RelationalExprOperator joinOp = parser.parse();
			addError(new UnsupportedOperationException("JOIN is not implemented yet"));
			return;
		}
		
		SchemaScope dataSourceScope = new SchemaScope(fromOp.getOutputSchema());

		// parse where
		Expression where = plainSelect.getWhere();
		if (where != null) {
			WhereConditionParser parser = new WhereConditionParser(this._sqlParser, plainSelectRootOp, dataSourceScope, where);
			try{
				RelationalExprOperator selectOp = parser.parse();
				plainSelectRootOp = selectOp;
			}
			catch(ParseErrorsException e){
				for(Exception err : e.getParseErrors())
					addError(err);
			}
		}
		
		// parse groupByColumnReferences
		List<TableColumn> groupByCols = null;
		@SuppressWarnings("unchecked") List<Expression> groupBy = plainSelect.getGroupByColumnReferences();
		if (groupBy != null) {
			GroupByColumnsParser parser = new GroupByColumnsParser(dataSourceScope, groupBy);
			try{
				groupByCols = parser.parse();
			}
			catch(UnsupportedOperationException e){
				addError(e);
			}
		}
		else {
			groupByCols = new ArrayList<TableColumn>();
		}
		
		// parse selectItems
		@SuppressWarnings("unchecked") List<SelectItem> selectItems = plainSelect.getSelectItems();
		assert selectItems != null;
		if (selectItems != null) {
			SelectItemsParser parser = new SelectItemsParser(this._sqlParser, plainSelectRootOp, null, selectItems, groupByCols);
			RelationalExprOperator projectOp = parser.parse();
			plainSelectRootOp = projectOp;
			if (parser.getOutputItemCount() > 0){
				if (this._subQuery){
					addError(new SemanticErrorException("OUTPUT() items cannot be in subquery"));
				}
				else{
					this._outputItems = CollectionUtil.copyArrayList(parser.getOutputItems());
				}
			}
			this._inputOrderSelectItems = parser.getItemsInputOrder();
		}
		
		// parse having
		SchemaScope resultScope = new SchemaScope(plainSelectRootOp.getOutputSchema());
		Expression having = plainSelect.getHaving();
		if (having != null) {
			WhereConditionParser parser = new WhereConditionParser(this._sqlParser, plainSelectRootOp, resultScope, having, true);
			try{
				RelationalExprOperator selectOp = parser.parse();
				plainSelectRootOp = selectOp;
			}
			catch(ParseErrorsException e){
				for(Exception err : e.getParseErrors())
					addError(err);
			}
		}
		
		@SuppressWarnings("unchecked") List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
		Top top = plainSelect.getTop();
		if (top != null && top.isRowCountJdbcParameter()){
			addError(new SemanticErrorException("Dynamic parameter (?) in TOP is not supported."));
		}

        // parse orderByElements
		if (orderByElements != null) {
			long topNumber = (top != null ? top.getRowCount() : 0);
			if(this._subQuery){
				addError(new UnsupportedOperationException("ORDER BY is not implemented in subquery."));
			}
			else{
				resultScope = new SchemaScope(plainSelectRootOp.getOutputSchema());
				try{
					OrderByColumnsParser parser = new OrderByColumnsParser(this._sqlParser, resultScope, orderByElements);
					Iterable<OrderByColumn> orderByColumns = parser.parse();
					RelationalExprOperator topOp = new TopOp(plainSelectRootOp, topNumber, orderByColumns);
					plainSelectRootOp = topOp;
				}
				catch(ParseErrorsException e){
					for(Exception err : e.getParseErrors())
						addError(err);
				}
			}
		}
		else if (top != null) {
			if(this._subQuery){
				addError(new UnsupportedOperationException("TOP is not implemented in subquery."));
			}
			else {
				RelationalExprOperator topOp = new TopOp(plainSelectRootOp, top.getRowCount(), null);
				plainSelectRootOp = topOp;
			}
		}

        // parse topGroupByColumns
        @SuppressWarnings("unchecked") List<Expression> topGroupBy = plainSelect.getTopGroupByColumns();
        if (topGroupBy != null) {
            if (top != null) {
                TopOp topOp = (TopOp)plainSelectRootOp;
                TopGroupByColumnsParser parser = new TopGroupByColumnsParser(this._sqlParser, resultScope);
                try{
                    Iterable<ColumnExpression> topGroupByCols = parser.parse(topGroupBy, topOp.getOrderByColumns());
                    topOp.setTopGroupByColumns(topGroupByCols);
                }
                catch(UnsupportedOperationException e){
                    addError(e);
                }
            }
            else {
                addError(new SemanticErrorException("TOP GROPU BY ... requires TOP <Number> ... in select."));
            }
        }

        // parse limit
		if (plainSelect.getLimit() != null) {
			addError(new UnsupportedOperationException("OFFSET/LIMIT is not implemented yet, use TOP instead."));
		}		
		
		// parse distinct 
		if (plainSelect.getDistinct() != null) {
			addError(new UnsupportedOperationException("DISTINCT is not implemented yet."));
		}
		
		this._oneSelectRootOp = plainSelectRootOp;
	}

	@Override
	public void visit(SetOperationList setOperationList) {
		addError(new UnsupportedOperationException("SetOperationList  is not implemented yet."));
	}

	@Override
	public void visit(WithItem withItem) {
		assert withItem != null;

		@SuppressWarnings("unchecked") List<SelectItem> cols = withItem.getWithItemList();
		WithColumnsParser withColumnParser = new WithColumnsParser();
		withColumnParser.parseWithColumns(cols);

		String tableName = withItem.getName();
		ArrayList<CaseIgnoredString> columnNames = withColumnParser.getColumnNames();
		SelectBody selectBody = withItem.getSelectBody();
		SelectParser parser = new SelectParser(this._sqlParser, null, selectBody);
		RelationalExprOperator operator = parser.parse();

		addError(new UnsupportedOperationException("WITH is not implemented yet."));
		// todo, create com.pplive.pike.metadata.Table according to tableName, columnNames, selectBody output schema
	}

/*	public void visit(Union union) {
		@SuppressWarnings("unchecked") List<PlainSelect> plainSelects = union.getPlainSelects();
		
		@SuppressWarnings("unchecked") List<OrderByElement> orderByElements = union.getOrderByElements();
//		if (orderByElements != null) {
//			OrderByColumnsParser orderByColumnParser = new OrderByColumnsParser();
//			orderByColumnParser.parseOrderByColumns(orderByElements);
//		}
		
		Limit limit = union.getLimit();
		addError(new UnsupportedOperationException("UNION is not implemented yet"));
		
		// TODO
		// create UnionOperator(), parse plainSelects, create LimitOperator(), OrderByOperator()
	}*/
	
}
