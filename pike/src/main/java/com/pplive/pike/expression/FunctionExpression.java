package com.pplive.pike.expression;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.parser.SemanticErrorException;
import com.pplive.pike.util.CollectionUtil;
import com.pplive.pike.util.ReflectionUtils;

import org.apache.log4j.Logger;
import storm.trident.tuple.TridentTuple;

public class FunctionExpression extends AbstractExpression {

	private final static Logger logger = Logger.getLogger(FunctionExpression.class);
	private static final long serialVersionUID = 1713219182567949312L;
	private final List<AbstractExpression> _params;
    public Iterable<AbstractExpression> getParams() {
        return this._params;
    }
    public List<AbstractExpression> getParamsRef() {
        return this._params;
    }
	public int getParamCount() {
		return this._params.size();
	}
	private final List<Class<?>> _argClasses;
	private final Class<? extends AbstractUDF> _methodClass;
	public Class<? extends AbstractUDF> getMethodClass(){
		return this._methodClass;
	}
	private final String _funcName;
	public String getFuncName(){
		return  this._funcName;
	}
	
	private transient Object _obj;
	private transient Method _method;
	
	public FunctionExpression(AbstractExpression param, Class<? extends AbstractUDF> clazz) {
		this(clazz.getSimpleName(), param, clazz);
	}
	
	public FunctionExpression(String funcName, AbstractExpression param, Class<? extends AbstractUDF> clazz) {
		this._funcName = funcName;
		this._params = new ArrayList<AbstractExpression>(1);
		this._params.add(param);
		this._argClasses = new ArrayList<Class<?>>(1);
		this._argClasses.add(param.exprType());
		this._methodClass = clazz;
		getFunctionMethod();
	}

	public FunctionExpression(Iterable<AbstractExpression> params, Class<? extends AbstractUDF> clazz) {
		this(clazz.getSimpleName(), params, clazz);
	}
	
	public FunctionExpression(String funcName, Iterable<AbstractExpression> params, Class<? extends AbstractUDF> clazz) {
		this._funcName = funcName;
		this._params = CollectionUtil.copyArrayList(params);
		this._argClasses = new ArrayList<Class<?>>(this._params.size());
		for(AbstractExpression expr : params)
			this._argClasses.add(expr.exprType());
		this._methodClass = clazz;
		getFunctionMethod();
	}

	public FunctionExpression(List<AbstractExpression> param, List<Class<?>> argClasses, Class<? extends AbstractUDF> clazz) {
		this._params = param;
		this._argClasses = argClasses;
		this._methodClass = clazz;
		this._funcName = clazz.getSimpleName();
		getFunctionMethod();
	}
	
	private void getFunctionMethod(){
		Method m = ReflectionUtils.tryGetEvalMethod(this._argClasses, this._methodClass);
		if (m == null){
			int n = 0;
			String s = "";
			for(Class<?> t : this._argClasses){
				n += 1; 
				if (n > 1) s += ", ";
				s += t.getSimpleName();
			}
			String msg = String.format("function %s exists, but compatible form of %s(%s) not found.", this._funcName, this._funcName, s);
			throw new SemanticErrorException(msg);
		}
		this._method = m;
	}

	@Override
	public void init() {
		for(AbstractExpression ie : _params){
			ie.init();
		}
		checkInitialized();
	}
	
	private void checkInitialized() {
		if (this._method == null)
			this._method = ReflectionUtils.getEvalMethod(_argClasses, _methodClass);
		if (this._obj == null)
			this._obj = ReflectionUtils.newInstance(this._methodClass);
		assert this._method != null;
		assert this._obj != null;
	}

	@Override
	public Object eval(TridentTuple tuple) {
		checkInitialized(); // trident reducer aggregator has no prepare(), so expression call init() won't be called
                            // if it's passed as parameter in aggregate function call
		
		// 实例化参数列表
		Object[] paramValues = new Object[_params.size()];

		for (int i = 0; i < _params.size(); i++) {
			paramValues[i] = _params.get(i).eval(tuple);
		}

		try {
		    return _method.invoke(_obj, paramValues);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(
					String.format("illegal argument exception."), e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(
					String.format("illegal access exception."), e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(
					String.format("invocation target exception."), e);
		}
	}
	
	@Override
	public Object visit(Object context, IExpressionVisitor visitor) {
		return visitor.visit(context, this);
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder(String.format("%s(", this._funcName));
		int n = 0;
		for(AbstractExpression expr : this._params){
			n += 1;
			if (n > 1) sb.append(", ");
			sb.append(expr);
		}
		sb.append(")");
		return sb.toString();
	}
	
	@Override
	public Class<?> exprType() {
		assert this._method != null;
		return this._method.getReturnType();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o == null || o.getClass() != this.getClass())
			return false;
		FunctionExpression other = (FunctionExpression)o;
		assert this._methodClass != null;
		if (this._methodClass != other._methodClass)
			return false;
		assert this._params != null;
		assert other._params != null;
		if (this._params.size() != other._params.size())
			return false;
		for(int n = 0; n < this._params.size(); n += 1){
			AbstractExpression l = this._params.get(n);
			AbstractExpression r = other._params.get(n);
			if (l.equals(r) == false)
				return false;
		}
		return true;
	}
	
	@Override
	public int hashCode(){
		return HashFailThrower.throwOnHash(this);
	}
}
