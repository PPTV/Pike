package com.pplive.pike.parser;

import java.util.ArrayList;
import com.pplive.pike.base.AbstractUdtf;
import com.pplive.pike.expression.FunctionExpression;
import com.pplive.pike.metadata.Table;

import net.sf.jsqlparser.statement.select.*;

class FromItemParser implements FromItemVisitor {

	private final PikeSqlParser _sqlParser;
	private final FromItem _fromItem;
	private RelationalExprOperator _fromRootOp;
	private ArrayList<Exception> _parseErrors = new ArrayList<Exception>();
	private void addError(Exception e){
		this._parseErrors.add(e);
	}
	
	public FromItemParser(PikeSqlParser sqlParser, FromItem fromItem) {
		assert sqlParser != null;
		assert fromItem != null;
		
		this._sqlParser = sqlParser;
		this._fromItem = fromItem;
	}
	
	public RelationalExprOperator parse() {
		this._fromRootOp = null;
		this._fromItem.accept(this);
		if (this._parseErrors.size() > 0){
			this._fromRootOp = null;
			throw new ParseErrorsException(this._parseErrors);
		}
		return this._fromRootOp;
	}

	public void visit(net.sf.jsqlparser.schema.Table sqlTable) {
		GlobalSchemaScope globalScope = this._sqlParser.createGlobalScope();
		if (sqlTable.getSchemaName() != null) {
			assert sqlTable.getSchemaName().isEmpty() == false;
			addError(new SemanticErrorException("schema name is not supported in Pike"));
			return;
		}
		Table table = globalScope.findTable(sqlTable.getName());
		if (table == null) {
			addError(new SemanticErrorException(String.format("table '%s' not found", sqlTable.getName())));
			return;
		}
		assert this._fromRootOp == null;
		this._fromRootOp = globalScope.getTableOp(table.getName());
		if (sqlTable.getAlias() != null) {
			assert sqlTable.getAlias().getName().isEmpty() == false;
			RenameOp renameOp = new RenameOp(this._fromRootOp, sqlTable.getAlias().getName());
			this._fromRootOp = renameOp;
		}

  //      List<LateralView> lateralViews = sqlTable.getLateralViews();
  //      parseLateralViews(lateralViews);
	}

    private static boolean not(boolean expr) { return !expr; }
/*
    private void parseLateralViews(List<LateralView> lateralViews) {
        if (lateralViews == null){
            return;
        }
        for(LateralView lv : lateralViews) {
            SchemaScope schemaScope  =  new SchemaScope(this._fromRootOp.getOutputSchema());
            FunctionParser funcParser = new FunctionParser(this._sqlParser, schemaScope, lv.getFunction(), null, false);
            try {
                FunctionExpression funcExpr = (FunctionExpression)funcParser.parse();
                Class<?> funcMethod = funcExpr.getMethodClass();
                if (not(AbstractUdtf.class.isAssignableFrom(funcMethod))) {
                    String msg = String.format("function %s is not a table generating function (udtf)", funcMethod.getSimpleName());
                    addError(new SemanticErrorException(msg));
                }
                LateralOp lateralOp = new LateralOp(this._fromRootOp, funcExpr, lv.getTableAlias(), lv.getColumnAliases());
                this._fromRootOp = lateralOp;
            }
            catch (ParseErrorsException pe) {
                for(Exception e : pe.getParseErrors()) {
                    this._parseErrors.add(e);
                }
            }
            catch (Exception e) {
                addError(e);
            }
        }
    }
	*/
	public void visit(SubSelect subSelect) {
		if (subSelect.getAlias() == null) {
			addError(new SemanticErrorException("SUBQUERY in From (...) has no result alias"));
		}
		
		SelectBody selectBody = subSelect.getSelectBody();
		SelectParser parser = new SelectParser(this._sqlParser, null, selectBody, true);
		try{
			RelationalExprOperator parsedOp = parser.parse();
			String table = subSelect.getAlias().getName();
			if (table == null) {
				table = InternalTableName.genTableName("").value();
			}
			RenameOp renameOp = new RenameOp(parsedOp, table);
			this._fromRootOp = renameOp;
		}
		catch(ParseErrorsException e){
			for(Exception err : e.getParseErrors())
				addError(err);
		}
  //      List<LateralView> lateralViews = subSelect.getLateralViews();
 //       parseLateralViews(lateralViews);
	}
	
	public void visit(SubJoin subjoin) {
		addError(new UnsupportedOperationException("join is not implemented yet"));
		// todo
	}

	@Override
	public void visit(LateralSubSelect lateralSubSelect) {
		addError(new UnsupportedOperationException("LateralSubSelect is not implemented yet"));
	}

	@Override
	public void visit(ValuesList valuesList) {
		addError(new UnsupportedOperationException("ValuesList is not implemented yet"));
	}
}
