package com.pplive.pike.expression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

import com.pplive.pike.base.AggregateMode;
import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.base.Period;
import com.pplive.pike.function.builtin.Avg;
import com.pplive.pike.function.builtin.BuiltinAggBase;
import com.pplive.pike.function.builtin.Count;
import com.pplive.pike.function.builtin.LinearCount;
import com.pplive.pike.function.builtin.LoglogAdaptiveCount;
import com.pplive.pike.function.builtin.MaxDouble;
import com.pplive.pike.function.builtin.MaxLong;
import com.pplive.pike.function.builtin.MinDouble;
import com.pplive.pike.function.builtin.MinLong;
import com.pplive.pike.function.builtin.SumDouble;
import com.pplive.pike.function.builtin.SumLong;
import com.pplive.pike.generator.trident.ICombinable;
import com.pplive.pike.generator.trident.ICombineReducible;
import com.pplive.pike.generator.trident.IReducible;
import com.pplive.pike.metadata.Column;

public class AggregateExpression extends AbstractExpression {

	private static final long serialVersionUID = -8932587804410143210L;

	protected final ArrayList<AbstractExpression> _params;
	public Iterable<AbstractExpression> getParams() {
		return this._params;
	}
	public int getParamCount() {
		return this._params.size();
	}
	
	protected final CaseIgnoredString _funcName;
	public CaseIgnoredString getFuncName() {
		return this._funcName;
	}
	protected final boolean _distinct;
	public boolean isDistinct() {
		return this._distinct;
	}
	
	protected Period _aggregatePeriod;
	public void setAggregatePeriod(Period period){
		this._aggregatePeriod = period;
	}
	public Period getAggregatePeriod() {
		return this._aggregatePeriod;
	}
	
	protected AggregateMode _aggMode = AggregateMode.Regular;
	public void setAggregateMode(AggregateMode mode){
		this._aggMode = mode;
	}
	public AggregateMode getAggregateMode() {
		return this._aggMode;
	}

	protected AggregateExpression(String funcName, List<AbstractExpression> params, boolean distinct) {
		if (funcName == null || funcName.isEmpty())
			throw new IllegalArgumentException("funcName cannot be null or empty");
		if (params == null)
			throw new IllegalArgumentException("params cannot be null");
				
		this._funcName = new CaseIgnoredString(funcName);
		this._params = new ArrayList<AbstractExpression>(params.size());
		this._params.addAll(params);
		this._distinct = distinct;
	}
	
	@Override
	public Object eval(TridentTuple tuple){
		throw new IllegalStateException("should never happen: it's impossible that AggregateExpression exists in a generated topology");
	}
	
	public boolean isBuiltin() { 
		return false;
	}
	
	public boolean isCombinable() {
		return false;
	}
	
	public boolean isCombineReducible() {
		return false;
	}
	
	public ICombinable<?, ?> createCombinable() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	public ICombineReducible<?, ?> createCombineReducible() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	public IReducible<?, ?> createDistinctReducible() {
		throw new UnsupportedOperationException("not implemented yet");
	}
	
	public static boolean isBuiltinAggregator(String funcName) {
		return BuiltinAggregatorExpression.isBuiltinAggregator(funcName);
	}
	
	public static AggregateExpression createBuiltin(String funcName, List<AbstractExpression> params, boolean distinct) {
		return BuiltinAggregatorExpression.create(funcName, params, distinct);
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder(300);
		if (getAggregateMode() != AggregateMode.Regular) {
			sb.append(String.format("%s('%s', ", this._aggMode, this._aggregatePeriod));
		}
		sb.append(String.format("%s(", this._funcName));
		if(this._distinct)
			sb.append("DISTINCT ");
		int n = 0;
		for(AbstractExpression expr : this._params){
			n += 1;
			if (n > 1)
				sb.append(", ");
			sb.append(expr);
		}
		sb.append(')');
		if (getAggregateMode() != AggregateMode.Regular) {
			sb.append(')');
		}
		return sb.toString();
	}

	@Override
	public Class<?> exprType() {
		// TODO 
		return Object.class;
	}
	
	@Override
	public Object visit(Object context, IExpressionVisitor visitor) {
		return visitor.visit(context, this);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o == this)
			return true;
		if ((o instanceof AggregateExpression) == false)
			return false;
		AggregateExpression other = (AggregateExpression)o;
		if (this._distinct != other._distinct)
			return false;
		assert this._funcName != null;
		if (this._funcName.equals(other._funcName) == false)
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

final class BuiltinAggregatorExpression extends AggregateExpression {

	private static final long serialVersionUID = -4422011526919913774L;
	
	public static BuiltinAggregatorExpression create(String funcName, List<AbstractExpression> params, boolean distinct) {
		
		if (isBuiltinAggregator(funcName) == false) {
			assert false;
			throw new IllegalArgumentException(String.format("should never happen: %s is not builtin aggregation function", funcName));
		}
		return new BuiltinAggregatorExpression(funcName, params, distinct);
	}
	
	private BuiltinAggregatorExpression(String funcName, List<AbstractExpression> params, boolean distinct) {
		super(funcName, params, distinct);
	}

	@Override
	public boolean isBuiltin() { 
		return true;
	}
	
	@Override
	public Class<?> exprType() {
		if (this._funcName.equalsString("COUNT") 
			|| this._funcName.equalsString("LinearCount")
			|| this._funcName.equalsString("LinearCountEx")
			|| this._funcName.equalsString("LoglogAdaptiveCount")){
			return Long.class;
		}
		
		assert this._params.size() == 1;
		if (this._funcName.equalsString("AVG")){
			return Double.class;
		}
		
		Class<?> t = this._params.get(0).exprType();
		if (Number.class.isAssignableFrom(t) == false) {
			assert false;
			throw new IllegalStateException(String.format("should never happen: %s() param is not number", this._funcName));
		}
		
		if (t == Byte.class || t == Short.class || t == Integer.class || t == Long.class)
			return Long.class;
		assert t == Float.class || t == Double.class;
		return Double.class;
	}

	@Override
	public boolean needConvertResultToExprType() {
		// return isCombinable() == false || this._funcName.equals("AVG");
		
		// after adding new interfaces/classes and improving trident ChainedAggregatorDeclarer/ChainedAggregatorImpl,
		// in aggregation we have extra final step to convert aggregate state class to final result type.
		// so this method is unnecessary to return true.
		return false;
	}
	
	@Override
	public boolean isCombinable() {
		return this._distinct == false
				|| this._funcName.equalsString("MAX")
				|| this._funcName.equalsString("MIN")
				|| this._funcName.equalsString("LinearCount")
				|| this._funcName.equalsString("LinearCountEx")
				|| this._funcName.equalsString("LoglogAdaptiveCount");
	}
	
	@Override
	public boolean isCombineReducible() {
		return this._distinct && isCombinable() == false;
	}
	
	@Override
	public ICombinable<?, ?> createCombinable() {
		if (isCombinable() == false) {
			assert false;
			throw new IllegalStateException("should never happen: createCombiner() can only be called when isCombinable() is true");
		}
		
		if (this._funcName.equalsString("COUNT")){
			assert this._params.size() >= 1;
			return Count.createCombinable(this._params);
		}
		
		if (this._funcName.equalsString("LinearCount") || this._funcName.equalsString("LinearCountEx") || this._funcName.equalsString("LoglogAdaptiveCount")){
			assert this._params.size() >= 2;
			AbstractExpression expr = this._params.get(0);
			assert expr instanceof ConstantExpression && expr.exprType() == Integer.class;
			ConstantExpression intExpr = (ConstantExpression)expr;
			Integer val = (Integer)intExpr.eval(null);
			
			ArrayList<AbstractExpression> params = new ArrayList<AbstractExpression>(this._params.size() - 1);
			for (int n = 1; n < this._params.size(); n += 1) {
				params.add(this._params.get(n));
			}
			if (this._funcName.equalsString("LinearCount") ) {
				return LinearCount.createCombinable(val, params);
			}
			else if (this._funcName.equalsString("LinearCountEx") ) {
				return LinearCount.createCombinableEx(val, params);
			}
			else {
				return LoglogAdaptiveCount.createCombinable(val, params);
			}
		}
		
		assert this._params.size() == 1;
		
		if (this._funcName.equalsString("SUM")) {
			if (this.exprType() == Long.class){
				return BuiltinAggBase.createCombinable(SumLong.class, this._params.get(0));
			}
			else{
				return BuiltinAggBase.createCombinable(SumDouble.class, this._params.get(0));
			}
		}
		
		if (this._funcName.equalsString("AVG")) {
			return BuiltinAggBase.createCombinable(Avg.class, this._params.get(0));
		}
		
		if (this._funcName.equalsString("MAX")) {
			if (this.exprType() == Long.class){
				return BuiltinAggBase.createCombinable(MaxLong.class, this._params.get(0));
			}
			else{
				return BuiltinAggBase.createCombinable(MaxDouble.class, this._params.get(0));
			}
		}
		
		if (this._funcName.equalsString("MIN")) {
			if (this.exprType() == Long.class){
				return BuiltinAggBase.createCombinable(MinLong.class, this._params.get(0));
			}
			else{
				return BuiltinAggBase.createCombinable(MinDouble.class, this._params.get(0));
			}
		}

		assert false;
		throw new IllegalStateException(String.format("should never happen: %s is not builtin aggregation function", this._funcName));
	}
	
	@Override
	public IReducible<?, ?> createDistinctReducible() {
		return createCombineReducible();
	}
	
	@Override
	public ICombineReducible<?, ?> createCombineReducible() {
		if (this._distinct == false) {
			assert false;
			throw new IllegalStateException("should never happen: createDistinctReducer() can only be called isDistinct() is true");
		}
		
		if (this._funcName.equalsString("COUNT")){
			assert this._params.size() >= 1;
			return Count.createDistinctReducible(this._params);
		}
		
		assert this._params.size() == 1;
		if (this._funcName.equalsString("SUM")) {
			if (this.exprType() == Long.class){
				return BuiltinAggBase.createDistinctReducible(SumLong.class, this._params.get(0));
			}
			else{
				return BuiltinAggBase.createDistinctReducible(SumDouble.class, this._params.get(0));
			}
		}
		
		if (this._funcName.equalsString("AVG")) {
			return BuiltinAggBase.createDistinctReducible(Avg.class, this._params.get(0));
		}
		
		if (this._funcName.equalsString("MAX")) {
			if (this.exprType() == Long.class){
				return BuiltinAggBase.createDistinctReducible(MaxLong.class, this._params.get(0));
			}
			else{
				return BuiltinAggBase.createDistinctReducible(MaxDouble.class, this._params.get(0));
			}
		}
		
		if (this._funcName.equalsString("MIN")) {
			if (this.exprType() == Long.class){
				return BuiltinAggBase.createDistinctReducible(MinLong.class, this._params.get(0));
			}
			else{
				return BuiltinAggBase.createDistinctReducible(MinDouble.class, this._params.get(0));
			}
		}

		assert false;
		throw new IllegalStateException(String.format("should never happen: %s is not builtin aggregation function", this._funcName));
	}

	private static final CaseIgnoredString[] _builtins = {
		new CaseIgnoredString("COUNT"),
		new CaseIgnoredString("SUM"),
		new CaseIgnoredString("MAX"),
		new CaseIgnoredString("MIN"),
		new CaseIgnoredString("AVG"),
		
		new CaseIgnoredString("LinearCount"),
		new CaseIgnoredString("LinearCountEx"),
		new CaseIgnoredString("LoglogAdaptiveCount"),
	};
	
	public static boolean isBuiltinAggregator(String funcName) {
		for(CaseIgnoredString s : _builtins) {
			if (s.equalsString(funcName))
				return true;
		}
		return false;
	}
	
}
