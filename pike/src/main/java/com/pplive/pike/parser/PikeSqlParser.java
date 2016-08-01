package com.pplive.pike.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.output.OutputTarget;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ConstantExpression;
import com.pplive.pike.metadata.TableManager;

public class PikeSqlParser implements StatementVisitor {

	public static void parseSQLSyntax(String sql) throws JSQLParserException {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");
		CCJSqlParserManager jsqlParser = new CCJSqlParserManager();
		jsqlParser.parse(new StringReader(sql));
	}
	
	public static Map<String, Object> parseSQLOptions(String sql) throws JSQLParserException {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");
		
		CCJSqlParserManager jsqlParser = new CCJSqlParserManager();
		Statement statement = jsqlParser.parse(new StringReader(sql));
		
		if ((statement instanceof Select) == false) {
			throw new ParseErrorsException(new SemanticErrorException("only support SELECT statement"));
		}
		
		Select select = (Select)statement;
		ArrayList<Exception> parseErrors = new ArrayList<Exception>();
		Map<String, Object> result = parseSetOptions(select.getOptionItems(), parseErrors);
		if (parseErrors.size() > 0){
			throw new ParseErrorsException(parseErrors);
		}
		return result;
	}

	public static LogicalQueryPlan parseSQL(String sql, TableManager tableManager) throws JSQLParserException {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");
		if (tableManager == null)
			throw new IllegalArgumentException("tableManager cannot be null");
		
		CCJSqlParserManager jsqlParser = new CCJSqlParserManager();
		Statement statement = jsqlParser.parse(new StringReader(sql));
		
		InternalTableName.reset();
		InternalColumnName.reset();
		PikeSqlParser parser = new PikeSqlParser(tableManager);
		statement.accept(parser);
		if (parser._parseErrors.size() > 0){
			throw new ParseErrorsException(parser._parseErrors);
		}
		LogicalQueryPlan queryPlan = new LogicalQueryPlan(parser._pikeOrStormOptions, parser._baseProcessPeriod,
							parser._parsedOp, parser._inputOrderSelectItems, parser._outputItems, parser._outputTargets);
		return queryPlan;
	}
	
	private final TableManager _tableManager;
	private ArrayList<RelationalExprSchema> _withTables;
	private ArrayList<RelationalExprOperator> _withTableOps;
	private RelationalExprOperator _parsedOp;

	private ArrayList<CaseIgnoredString> _inputOrderSelectItems;
	private Iterable<TransformField> _outputItems; // the OUTPUT(...) items

	private Iterable<OutputTarget> _outputTargets;

	private Period _baseProcessPeriod;
	Period getProcessPeriod() {
		return this._baseProcessPeriod;
	}
	
	private Map<String, Object> _pikeOrStormOptions;
	
	private ArrayList<Exception> _parseErrors = new ArrayList<Exception>();
	private void addError(Exception e){
		this._parseErrors.add(e);
	}
	
	private PikeSqlParser(TableManager tableManager) {
		assert tableManager != null;
		this._tableManager = tableManager;
	}
	
	public GlobalSchemaScope createGlobalScope() {
		String[] names = this._tableManager.getTableNames();
		ArrayList<RelationalExprSchema> tables = new ArrayList<RelationalExprSchema>(names.length + 10);
		ArrayList<RelationalExprOperator> tableOps = new ArrayList<RelationalExprOperator>(names.length + 10);
		for(int n = 0; n < names.length; n +=1){
			tables.add(new RelationalExprSchema(this._tableManager.getTable(names[n])));
			tableOps.add(new LeafTableOp(tables.get(n)));
		}
		if (this._withTables != null) {
			assert this._withTableOps != null && this._withTableOps.size() == this._withTables.size();
			tables.addAll(this._withTables);
			tableOps.addAll(this._withTableOps);
		}
		return new GlobalSchemaScope(tables, tableOps);
	}
	
	// visit methods of StatementVisitor
	public void visit(Select select) {

		this._pikeOrStormOptions = parseSetOptions(select.getOptionItems(), this._parseErrors);
		
		// parse period 
		net.sf.jsqlparser.statement.select.Period period = select.getPeriod();
		if (period != null) {
			this._baseProcessPeriod = Period.parse(period.getPeriodString());
		}
		else{
			addError(new SemanticErrorException("base process period missing: WithPeriod <XX>s|<XX>m|<XX>h|<XX>d SELECT ..."));
		}
		
		SelectBody selectBody = select.getSelectBody();
		SelectParser parser = new SelectParser(this, null, selectBody);
		try{
			this._parsedOp = parser.parse();
			if (parser.getOutputItemCount() > 0){
				this._outputItems = parser.getOutputItems();
			}
			else{
				this._outputItems = new ArrayList<TransformField>(0);
			}
			if (parser.getOutputTargetCount() > 0){
				this._outputTargets = parser.getOutputTargets();
			}
			else{
				this._outputTargets = new ArrayList<OutputTarget>(0);
			}
			this._inputOrderSelectItems = parser.getItemsInputOrder();
		}
		catch(ParseErrorsException e){
			for(Exception err : e.getParseErrors())
				addError(err);
		}
	}
	
	private static Map<String, Object> parseSetOptions(Map<String, Object> options, ArrayList<Exception> parseErrors){
		HashMap<String, Object> parsedOptions = new HashMap<String, Object>();
		if (options == null){
			return parsedOptions;
		}
		for(Map.Entry<String, Object> kv : options.entrySet()){
			assert kv.getValue() instanceof Expression;
			ConstantExpressionParser exprParser = new ConstantExpressionParser((Expression)kv.getValue());
			AbstractExpression expr = exprParser.parse();
			assert expr instanceof ConstantExpression;
			Object val = expr.eval(null);
			parsedOptions.put(kv.getKey(), val);
			if (val == null) {
				parseErrors.add(new SemanticErrorException("set option to null is not supported"));
			}
			else {
				Class<?> t = val.getClass();
				if (t != Boolean.class && t != String.class
						&& t != Integer.class && t != Long.class
						&& t != Float.class && t != Double.class){
					parseErrors.add(new SemanticErrorException("set option value can only be boolean, string, integer or float number"));
				}
			}
		}
		return parsedOptions;
	}

	public void visit(Delete delete) {
		addError(new SemanticErrorException("DELETE is not supported in Pike, only support SELECT statement"));
	}
	
	public void visit(Update update) {
		addError(new SemanticErrorException("UPDATE is not supported in Pike, only support SELECT statement"));
	}
	
	public void visit(Insert insert) {
		addError(new SemanticErrorException("INSERT is not supported in Pike, only support SELECT statement"));
	}
	
	public void visit(Replace replace) {
		addError(new SemanticErrorException("REPLACE is not supported in Pike, only support SELECT statement"));
	}
	
	public void visit(Drop drop) {
		addError(new SemanticErrorException("DROP is not supported in Pike, only support SELECT statement"));
	}
	
	public void visit(Truncate truncate) {
		addError(new SemanticErrorException("TRUNCATE is not supported in Pike, only support SELECT statement"));
	}

	@Override
	public void visit(CreateIndex createIndex) {
		addError(new SemanticErrorException("CREATE INDEX is not supported in Pike, only support SELECT statement"));
	}

	public void visit(CreateTable createTable) {
		addError(new SemanticErrorException("CREATE is not supported in Pike, only support SELECT statement"));
	}

	@Override
	public void visit(CreateView createView) {
		addError(new SemanticErrorException("CREATE VIEW is not supported in Pike, only support SELECT statement"));
	}

	@Override
	public void visit(Alter alter) {
		addError(new SemanticErrorException("ALTER is not supported in Pike, only support SELECT statement"));
	}

	@Override
	public void visit(Statements statements) {
		addError(new SemanticErrorException("Multi Statement not supported in Pike, only support SELECT statement"));
	}
}
