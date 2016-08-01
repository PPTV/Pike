package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.pplive.pike.base.*;
import com.pplive.pike.exec.output.OutputContext;
import com.pplive.pike.util.CollectionUtil;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.AggregateExpression;
import com.pplive.pike.expression.ConstantExpression;
import com.pplive.pike.expression.ExpressionAggregateCallInspector;
import com.pplive.pike.expression.ExpressionColumnRefInspector;
import com.pplive.pike.expression.IfExpression;
import com.pplive.pike.expression.OutputExpression;
import com.pplive.pike.function.builtin.BuiltinFunctions;
import com.pplive.pike.function.builtin.IBuiltinFunctionParser;

class FunctionParser {

	private final PikeSqlParser _sqlParser;
	private final SchemaScope _schemaScope;
	private final Function _function;
	private final ISizeAwareIterable<String> _callingChainFunctions;
	private final boolean _isAggregateFuncCall;
	
	private AbstractExpression _parsedExpr;
	
	private ArrayList<Exception> _parseErrors = new ArrayList<Exception>();
	private void addError(Exception e){
		this._parseErrors.add(e);
	}
	
	public FunctionParser(PikeSqlParser sqlParser, SchemaScope schemaScope, Function function, ISizeAwareIterable<String> callingChainFunctions, boolean isAggregateFuncCall) {
		assert sqlParser != null;
		assert function != null;
		assert schemaScope != null;

		this._sqlParser = sqlParser;
		this._schemaScope = schemaScope;
		this._function = function;
		this._callingChainFunctions = callingChainFunctions;
		this._isAggregateFuncCall = isAggregateFuncCall;
	}
	
	private ExpressionParser newExprParser(Expression expr){
		return newExprParser(expr, this._isAggregateFuncCall);
	}
	
	private ExpressionParser newExprParser(Expression expr, boolean isAggregateFunc){

        ArrayList<String> callingChainFunctions;
        if (this._callingChainFunctions == null) {
            callingChainFunctions = new ArrayList<String>(1);
        }
        else {
            callingChainFunctions = CollectionUtil.copyArrayList(this._callingChainFunctions);
        }
        callingChainFunctions.add(this._function.getName());
		return new ExpressionParser(this._sqlParser, this._schemaScope, expr, SizeAwareIterable.of(callingChainFunctions), isAggregateFunc);
	}
	
	public AbstractExpression parse() {
		AbstractExpression expr = parseImpl();
		if (this._parseErrors.size() > 0) {
			throw new ParseErrorsException(this._parseErrors);
		}
		return expr;
	}
	
	private AbstractExpression parseImpl() {
		String funcName = this._function.getName();
		if (isPseudoFunction(funcName)) {
			return parsePseudoFunction(this._function);
		}
		
		if (AggregateExpression.isBuiltinAggregator(funcName)) {
			return parseBuiltinAggregator(this._function);
		}
		
		if (this._function.getParameters() == null && this._function.isAllColumns()) {
			addError(new SemanticErrorException("all columns parameter ('*') can only used in COUNT() function"));
			return null;
		}
		
		if (BuiltinFunctions.isBuiltinFunction(funcName)) {
			return parseBuiltinFunction(this._function);
		}
		
		String msg = String.format("%s(): function not found, user defined function/aggregation call is not implemented yet.", funcName);
		addError(new UnsupportedOperationException(msg));
		return null;
	}
	
	private AbstractExpression parseBuiltinAggregator(Function function) {
		assert function != null;
		String funcName = function.getName();
		if (this._callingChainFunctions != null && this._callingChainFunctions.size() > 0 && this._isAggregateFuncCall){
			String msg = String.format("%s() cannot contain parameter as aggregate function call %s(), "
					+ "it's also an aggregate function or it's already in an aggregate function call.", SizeAwareIterable.last(this._callingChainFunctions), funcName);
			addError(new SemanticErrorException(msg));
			return null;
		}
		
		final CaseIgnoredString fn = new CaseIgnoredString(funcName);
		
		if(fn.equalsString("COUNT")){
			return parseCount(function);
		}
		if(fn.equalsString("LinearCount") || fn.equalsString("LinearCountEx") || fn.equalsString("LoglogAdaptiveCount")){
			return parseLinearOrLoglogCount(function);
		}
		else{
			return parseSumAndSimilar(function);
		}
	}
	
	private boolean isPseudoFunction(String funcName) {
		return funcName.equalsIgnoreCase("if")
                || funcName.equalsIgnoreCase("output")
                || funcName.equalsIgnoreCase("outputctx")
				|| funcName.equalsIgnoreCase("move")
				|| funcName.equalsIgnoreCase("accumulate");
	}
	
	private AbstractExpression parsePseudoFunction(Function function) {
		String funcName = function.getName();
		
		if (funcName.equalsIgnoreCase("if")) {
			return parseIf(this._function);
		}

        if (funcName.equalsIgnoreCase("output")) {
            return parseOutput(this._function);
        }
        if (funcName.equalsIgnoreCase("outputctx")) {
            return parseOutputContext(this._function);
        }


        if (funcName.equalsIgnoreCase("move")
			|| funcName.equalsIgnoreCase("accumulate")) {
			return parseMoveOrAccumulate(this._function);
		}
		
		throw new RuntimeException("should be impossible: pseudo function unrecognized");
	}
	
	private AbstractExpression parseCount(Function function) {
		assert function != null && new CaseIgnoredString(function.getName()).equalsString("COUNT");
		
		ExpressionList paramsList = function.getParameters();
		if (paramsList == null) {
			assert function.isAllColumns();
			if (function.isDistinct()) {
				addError(new SemanticErrorException("COUNT(DISTINCT *) is not allowed."));
			}
			ArrayList<AbstractExpression> paramExprs = new ArrayList<AbstractExpression>(1);
			paramExprs.add(new ConstantExpression(Long.valueOf(1)));
			return AggregateExpression.createBuiltin("COUNT", paramExprs, false);
		}
		
		@SuppressWarnings("unchecked") List<Expression> params = paramsList.getExpressions();
		if(params.size() == 0){
			addError(new SemanticErrorException("function COUNT() require 1+ parameters"));
		}
		
		ArrayList<AbstractExpression> paramExprs = new ArrayList<AbstractExpression>(params.size());
		for(Expression expr : params) {
			AbstractExpression parsed = newExprParser(expr, true).parse();
			paramExprs.add(parsed);
		}
		
		return AggregateExpression.createBuiltin("COUNT", paramExprs, function.isDistinct());
	}
	
	private AbstractExpression parseLinearOrLoglogCount(Function function) {
		assert function != null;
		final CaseIgnoredString fn = new CaseIgnoredString(function.getName());
		
		ExpressionList paramsList = function.getParameters();
		if (paramsList == null) {
			addError(new SemanticErrorException(String.format("%s(*) is not supported", fn)));
			ArrayList<AbstractExpression> paramExprs = new ArrayList<AbstractExpression>(1);
			paramExprs.add(new ConstantExpression(Long.valueOf(1)));
			return AggregateExpression.createBuiltin(fn.value(), paramExprs, true);
		}
		
		@SuppressWarnings("unchecked") List<Expression> params = paramsList.getExpressions();
		if(params.size() < 2){
			addError(new SemanticErrorException(String.format("function %s() require 2+ parameters", fn)));
		}

		ArrayList<AbstractExpression> paramExprs = new ArrayList<AbstractExpression>(params.size());
		for(Expression expr : params) {
			AbstractExpression parsed = newExprParser(expr, true).parse();
			paramExprs.add(parsed);
		}
		AbstractExpression firstExpr = paramExprs.get(0);
		if ((firstExpr instanceof ConstantExpression) == false){
			addError(new SemanticErrorException(String.format("function %s() first parameter must be constant integer", fn)));
		}
		else if (firstExpr.exprType() != Integer.class){
			addError(new SemanticErrorException(String.format("function %s() first parameter must be constant Integer", fn)));
		}
		else {
			Integer val = (Integer)((ConstantExpression)firstExpr).eval(null);
			if (fn.equalsString("LoglogAdaptiveCount") && val >= 32){
				addError(new SemanticErrorException(String.format("function %s() first parameter must be < 32", fn)));
			}
		}
		
		return AggregateExpression.createBuiltin(fn.value(), paramExprs, true);
	}
	
	private AbstractExpression parseSumAndSimilar(Function function) {
		assert function != null && AggregateExpression.isBuiltinAggregator(function.getName());
		
		ExpressionList paramsList = function.getParameters();
		if (paramsList == null) {
			assert function.isAllColumns();
			addError(new SemanticErrorException("all columns parameter ('*') can only used in COUNT() function"));
			return null;
		}
		@SuppressWarnings("unchecked") List<Expression> params = paramsList.getExpressions();
		
		if(params.size() != 1){
			String msg = String.format("function %s() require one and only one parameter", function.getName());
			addError(new SemanticErrorException(msg));
		}
		
		Expression expr = params.get(0);
		AbstractExpression parsed = newExprParser(expr, true).parse();
		if (Number.class.isAssignableFrom(parsed.exprType()) == false) {
			addError(new SemanticErrorException(String.format("function %s() parameter must be number type", function.getName())));
			return null;
		}
		ArrayList<AbstractExpression> paramExprs = new ArrayList<AbstractExpression>(1);
		paramExprs.add(parsed);
				
		return AggregateExpression.createBuiltin(function.getName(), paramExprs, function.isDistinct());
	}
	
	private AbstractExpression parseBuiltinFunction(Function function) {
		assert function != null;
		
		IBuiltinFunctionParser parser = BuiltinFunctions.getFunctionParser(function.getName());
		if (parser == null){
			String msg = String.format("builtin function %s() is not implemented yet.", function.getName());
			addError(new UnsupportedOperationException(msg));
			return null;
		}
		
		ArrayList<AbstractExpression> paramExprs = parseFuncParams(function);
		if (paramExprs == null){
			return null;
		}

		try{
			return parser.parse(function.getName(), paramExprs);
		}
		catch(ParseErrorsException e){
			for(Exception ex : e.getParseErrors()){
				addError(ex);
			}
			return null;
		}

	}
	
	private ArrayList<AbstractExpression> parseFuncParams(Function function) {
		ExpressionList paramsList = function.getParameters();
		ArrayList<AbstractExpression> paramExprs;
		if (paramsList != null){
			@SuppressWarnings("unchecked") List<Expression> params = paramsList.getExpressions();
			
			paramExprs = new ArrayList<AbstractExpression>(params.size());
			for(int n = 0; n < params.size(); n += 1){
				Expression expr = params.get(n);
				try{
					AbstractExpression paramExpr = newExprParser(expr).parse();
					paramExprs.add(paramExpr);
				}
				catch(ParseErrorsException e){
					for(Exception ex : e.getParseErrors()){
						addError(ex);
					}
					return null;
				}
			}
		}
		else{
			paramExprs = new ArrayList<AbstractExpression>(0);
		}
		return paramExprs;
	}
	
	private AbstractExpression parseIf(Function function) {
		assert function != null && new CaseIgnoredString(function.getName()).equalsString("IF");
		
		ExpressionList paramsList = function.getParameters();
		if(paramsList == null || paramsList.getExpressions().size() != 3){
			addError(new SemanticErrorException("function IF() require 3 parameters"));
			return null;
		}
		
		@SuppressWarnings("unchecked") List<Expression> params = paramsList.getExpressions();
		Expression expr = params.get(0);
		AbstractExpression condition = newExprParser(expr).parse();
		expr = params.get(1);
		AbstractExpression trueResult = newExprParser(expr).parse();
		expr = params.get(2);
		AbstractExpression falseResult = newExprParser(expr).parse();
		
		if (ExpressionAggregateCallInspector.containsAggregateCall(trueResult)
				|| ExpressionAggregateCallInspector.containsAggregateCall(falseResult)){
			addError(new SemanticErrorException("IF() expression result cannot contain aggregate function call"));
			return null;
		}
		
		if (IfExpression.isCompatibleType(trueResult, falseResult) == false) {
			String msg = String.format("IF() expression result type inconsistent, true branch is %s, false branch is %s",
										trueResult.exprType().getSimpleName(), falseResult.exprType().getSimpleName());
			addError(new SemanticErrorException(msg));
			return null;
		}
		
		return new IfExpression(condition, trueResult, falseResult);
	}
	
	private AbstractExpression parseOutput(Function function) {
		assert function != null && new CaseIgnoredString(function.getName()).equalsString("OUTPUT");
		
		ExpressionList paramsList = function.getParameters();
		if(paramsList == null || paramsList.getExpressions().size() != 1){
			addError(new SemanticErrorException("function OUTPUT() require one and only one parameter"));
			return null;
		}
		
		@SuppressWarnings("unchecked") List<Expression> params = paramsList.getExpressions();
		Expression expr = params.get(0);
		AbstractExpression parsedExpr = newExprParser(expr).parse();
		
		if (ExpressionColumnRefInspector.containsColumnOrVariableReference(parsedExpr)
				|| ExpressionAggregateCallInspector.containsAggregateCall(parsedExpr)){
			addError(new SemanticErrorException("OUTPUT() cannot reference any column, variable or aggregate function call"));
			return null;
		}
		
		return new OutputExpression(parsedExpr);
	}

    private static boolean not(boolean expr) {
        return !expr;
    }

    private AbstractExpression parseOutputContext(Function function) {
        assert function != null && new CaseIgnoredString(function.getName()).equalsString("OUTPUTCTX");

        ExpressionList paramsList = function.getParameters();
        if(paramsList != null && paramsList.getExpressions().size() > 0){
            addError(new SemanticErrorException("function OUTPUTCTX() have no parameter"));
            return null;
        }

        // check in OUTPUT() calling chain
        boolean inOutputCallingChain = false;
        if (this._callingChainFunctions != null) {
            for(String s : this._callingChainFunctions) {
                if (s.equalsIgnoreCase("OUTPUT")){
                    inOutputCallingChain = true;
                    break;
                }
            }
        }
        if (not(inOutputCallingChain)) {
            addError(new SemanticErrorException("OUTPUTCTX() can only be in OUTPUT() calling"));
            return null;
        }

        // the runtime will dynamically set proper value when topology is running.
        // see TopologyOutputManager.setOutputContext()
        return ConstantExpression.NullValue(OutputContext.class);
    }

    private AbstractExpression parseMoveOrAccumulate(Function function) {
		assert function != null;
		assert function.getName().equalsIgnoreCase("Move") || function.getName().equalsIgnoreCase("Accumulate");
		
		final boolean isAccumulate = function.getName().equalsIgnoreCase("Accumulate");
		
		ArrayList<AbstractExpression> params = parseFuncParams(function);
		if (params == null){
			return null;
		}

		if(params.size() != 2){
			String msg = String.format("function %s() require 2 parameters", function.getName());
			addError(new SemanticErrorException(msg));
			return null;
		}
		
		Period aggregatePeriod = parseAggregatePeriod(function, params.get(0));
		if (aggregatePeriod == null){
			return null;
		}
		
		AbstractExpression expr = params.get(1);
		if ((expr instanceof AggregateExpression) == false) {
			String msg = String.format("function %s() second parameter MUST be a aggregate function call. (count, sum, etc).", function.getName());
			addError(new SemanticErrorException(msg));
			return null;
		}
		
		AggregateExpression aggExpr = (AggregateExpression)expr;
		if (aggExpr.isCombinable() == false && aggExpr.isCombineReducible() == false){
			assert false;
			String msg = String.format("function %s() is neither ICombinable<> nor ICombineReducible<>, cannot be used in %s().",
									aggExpr.getFuncName(), function.getName());
			addError(new SemanticErrorException(msg));
		}
		aggExpr.setAggregateMode(isAccumulate ? AggregateMode.Accumulating : AggregateMode.Moving);
		aggExpr.setAggregatePeriod(aggregatePeriod);
		
		return aggExpr;
	}
	
	private Period parseAggregatePeriod(Function function, AbstractExpression expr) {
		if ( (expr instanceof ConstantExpression) == false
				|| (((ConstantExpression)expr).eval(null) instanceof String == false) ) {
			String msg = String.format("function %s() first parameter MUST be a string constant represent aggregate period ('10s', '3m', '4h', etc).", function.getName());
			addError(new SemanticErrorException(msg));
			return null;
		}
		
		String periodString = (String)((ConstantExpression)expr).eval(null);
        Period aggregatePeriod = Period.parse(periodString);
        if (aggregatePeriod == null) {
            String msg = String.format("function %s() first parameter MUST be a string constant represent aggregate period ('10s', '3m', '4h', etc).", function.getName());
            addError(new SemanticErrorException(msg));
            return null;
        }

		Period basePeriod = this._sqlParser.getProcessPeriod();
		assert basePeriod != null;
        int seconds = aggregatePeriod.periodSeconds();
		if (seconds < basePeriod.periodSeconds() || seconds % basePeriod.periodSeconds() != 0) {
			String msg = String.format("function %s(): first parameter aggregate period(%s) must be N times of base process period in SQL begin 'withperiod ...' (%s seconds), N >= 1.",
					function.getName(), periodString, basePeriod);
			addError(new SemanticErrorException(msg));
			return null;
		}
		return aggregatePeriod;
	}
}
