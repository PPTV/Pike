package com.pplive.pike.function.builtin;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ConstantExpression;
import com.pplive.pike.expression.FunctionExpression;
import com.pplive.pike.parser.ParseErrorsException;
import com.pplive.pike.parser.SemanticErrorException;
import com.pplive.pike.util.ReflectionUtils;

public final class Convert {
	
	public static class Parser implements IBuiltinFunctionParser {
		private ArrayList<Exception> _parseErrors = new ArrayList<Exception>();
		private void addError(Exception e){
			this._parseErrors.add(e);
		}
		
		public AbstractExpression parse(String funcName, List<AbstractExpression> params) {
			AbstractExpression expr = parseImpl(funcName, params);
			if (this._parseErrors.size() > 0) {
				throw new ParseErrorsException(this._parseErrors);
			}
			return expr;
		}

		public AbstractExpression parseImpl(String funcName, List<AbstractExpression> params) {
			CaseIgnoredString functionName = new CaseIgnoredString(funcName);
			
			String targetType;
			AbstractExpression targetExpr;
			Class<? extends AbstractUDF> convertClass;
			
			if (functionName.equalsString("CONVERT")) {
				if(params.size() != 2){
					addError(new SemanticErrorException("function CONVERT() require 2 parameters"));
					return null;
				}
				AbstractExpression expr = params.get(0);
				if ( (expr instanceof ConstantExpression) == false
						|| (((ConstantExpression)expr).eval(null) instanceof String == false) ) {
					String msg = "function CONVERT() first parameter MUST be a string constant represent a supported type."
								+ " (Boolean, String, Byte, Short, Int, Long, Float, Double)";
					addError(new SemanticErrorException(msg));
					return null;
				}
				
				targetType = (String)((ConstantExpression)expr).eval(null);
				if (Convert.isSupportedTargetType(targetType) == false){
					String msg = "CONVERT() type is not supported. all supported are: Boolean, String, Byte, Short, Int, Long, Float, Double, Map";
					addError(new SemanticErrorException(msg));
					return null;
				}
				
				convertClass = Convert.getConvertFunction(targetType);
				targetExpr = params.get(1);

			}
			else {
				assert functionName.equalsString("Boolean")
						|| functionName.equalsString("String") || functionName.equalsString("Str")
						|| functionName.equalsString("Byte")
						|| functionName.equalsString("Short")
						|| functionName.equalsString("Int") || functionName.equalsString("Integer")
						|| functionName.equalsString("Long")
						|| functionName.equalsString("Float")
						|| functionName.equalsString("Double")
						|| functionName.equalsString("Map")
						;
				if(params.size() != 1){
					addError(new SemanticErrorException(String.format("function %s() require 2 parameters", funcName)));
					return null;
				}
				targetType = functionName.value();
				targetExpr = params.get(0);
				convertClass = Convert.getConvertFunction(functionName.value());
			}
			
			if (targetExpr == null)
				return null;
			
			// optimize for converting a constant
			if (targetExpr.getClass() == ConstantExpression.class){
				try {
					return Convert.convertConstant(targetType, convertClass, (ConstantExpression)targetExpr);
				}
				catch(SemanticErrorException e){
					addError(e);
					return null;
				}
			}
					
			return new FunctionExpression(funcName, targetExpr, convertClass);
		}
	}

	private static final CaseIgnoredString[] _types = {
		new CaseIgnoredString("Boolean"),
		new CaseIgnoredString("String"), new CaseIgnoredString("Str"),
		new CaseIgnoredString("Byte"),
		new CaseIgnoredString("Short"),
		new CaseIgnoredString("Int"), new CaseIgnoredString("Integer"),
		new CaseIgnoredString("Long"),
		new CaseIgnoredString("Float"),
		new CaseIgnoredString("Double"),
		new CaseIgnoredString("Map")
	};

	private static boolean isSupportedTargetType(String type) {
		if (type == null || type.isEmpty())
			throw new IllegalArgumentException("type cannot be null or empty");
		
		for(CaseIgnoredString s : _types) {
			if (s.equalsString(type))
				return true;
		}
		return false;
	}
	
	private static Class<? extends AbstractUDF> getConvertFunction(String type) {
		return getConvertFunction(new CaseIgnoredString(type));
	}
	
	private static Class<? extends AbstractUDF> getConvertFunction(CaseIgnoredString type) {
		if (type == null || type.isEmpty())
			throw new IllegalArgumentException("type cannot be null or empty");

		if (type.equalsString("Boolean")){
			return ConvertToBoolean.class;
		}
		if (type.equalsString("String")){
			return ConvertToString.class;
		}
		if (type.equalsString("Byte")){
			return ConvertToByte.class;
		}
		if (type.equalsString("Short")){
			return ConvertToShort.class;
		}
		if (type.equalsString("Int") || type.equalsString("Integer")){
			return ConvertToInt.class;
		}
		if (type.equalsString("Long")){
			return ConvertToLong.class;
		}
		if (type.equalsString("Float")){
			return ConvertToFloat.class;
		}
		if (type.equalsString("Double")){
			return ConvertToDouble.class;
		}
		if (type.equalsString("Map")){
			return ConvertToMap.class;
		}
		String msg = String.format("convert type %s is not recognized. supported are: Boolean, String, Byte, Short, Int, Long, Float, Double, Map", type);
		throw new SemanticErrorException(msg);
	}
	
	private static ConstantExpression convertConstant(String targetType, Class<?> convertClass, ConstantExpression expr){
		String msg = String.format("CONVERT('%s', %s) failed", targetType, expr.eval(null));
		Method method = ReflectionUtils.getEvalMethod(Arrays.asList(new Class<?>[]{expr.exprType()}), convertClass);
		Object obj = ReflectionUtils.newInstance(convertClass);
		try {
			Object o = method.invoke(obj, expr.eval(null));
			if (o != null){
				return new ConstantExpression(o);
			}
			else {
				return ConstantExpression.NullValue(method.getReturnType());
			}
		}
		catch(InvocationTargetException e){
			throw new SemanticErrorException(msg);
		}
		catch (IllegalAccessException e) {
			throw new SemanticErrorException(msg);
		}
	}
}
