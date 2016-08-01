package com.pplive.pike.function.builtin;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.generator.trident.ICombinable;
import com.pplive.pike.generator.trident.ICombineReducible;
import com.pplive.pike.util.ReflectionUtils;

public abstract class BuiltinAggBase<TState, TResult> implements Serializable {

	private static final long serialVersionUID = 6656548878245871301L;

	public abstract TState convert(Object o);
	
	public TState init() { return null; }
	
	public TState combine(TState left, TState right){
		if (right == null) return left;
		if (left == null) return right;
		return combineNonNull(left, right);
	}
	protected abstract TState combineNonNull(TState left, TState right);
	
	public TState accumulate(TState accumulatedValue, Object val){
		if (val == null) return accumulatedValue;
		if (accumulatedValue == null) return convert(val);
		return accumulateNonNull(accumulatedValue, val);
	}
	protected TState accumulateNonNull(TState accumulatedValue, Object val){
		TState right = convert(val);
		return combine(accumulatedValue, right);
	}

	public TResult finish(TState state){
		if (state == null) return null;
		return finishNonNull(state);
	}
	protected abstract TResult finishNonNull(TState state);
	
	protected static Long convertLong(Object o){
		if (o == null)
			return null;
		if (o instanceof Long)
			return (Long)o;
		if ((o instanceof Number) == false)
			return null;
		return ((Number)o).longValue();
	}

	protected static Double convertDouble(Object o){
		if (o == null)
			return null;
		if (o instanceof Double)
			return (Double)o;
		if ((o instanceof Number) == false)
			return null;
		return ((Number)o).doubleValue();
	}

	public static<TState, TResult, T extends BuiltinAggBase<TState, TResult>>
		ICombinable<TState, TResult> createCombinable(Class<T> type, AbstractExpression param) {
		return new BuiltinAggCombiner<TState, TResult, T>(type,  param);
	}
	
	public static<TState, TResult, T extends BuiltinAggBase<TState, TResult>>
		ICombineReducible<DistinctState<TState>, TResult> createDistinctReducible(Class<T> type, AbstractExpression param) {
		return new BuiltinDistinctAggReducer<TState, TResult, T>(type,  param);
	}
}

class BuiltinAggCombiner<TState, TResult, T extends BuiltinAggBase<TState, TResult>>
		implements ICombinable<TState, TResult> {
	
	private static final long serialVersionUID = -7442476550703110154L;

	private final T _aggObj;
	private AbstractExpression _param;
	
	public BuiltinAggCombiner(Class<T> type, AbstractExpression param) {
		assert type != null;
		assert param != null;

		this._aggObj = ReflectionUtils.newInstance(type);
		this._param = param;
	}

    @Override
    public ICombinable<TState, TResult> cloneForStateCombine(AbstractExpression stateExpr) {
        return new BuiltinAggCombiner(this._aggObj.getClass(), stateExpr);
    }

    @Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public TState evalToCombinable(TridentTuple tuple) {
		Object paramValue = this._param.eval(tuple);
		if (paramValue == null)
			return null;
    	return this._aggObj.convert(paramValue);
	}

	@Override
	public TState init() {
		return this._aggObj.init();
	}

	@Override
	public TState combine(TState left, TState right) {
    	return this._aggObj.combine(left, right);
	}
	
	@Override
	public TResult finish(TState state){
		return this._aggObj.finish(state);
	}
}

class BuiltinDistinctAggReducer<TState, TResult, T extends BuiltinAggBase<TState, TResult>>
		implements ICombineReducible<DistinctState<TState>, TResult> {

	private static final long serialVersionUID = 19144376958056055L;

	private final AbstractExpression _param;
	private final T _aggObj;

	public BuiltinDistinctAggReducer(Class<T> type, AbstractExpression param) {
		assert type != null && param != null;

		this._param = param;
		this._aggObj = ReflectionUtils.newInstance(type);
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}


	@Override
	public DistinctState<TState> init() {
		TState init = this._aggObj.init();
		return new DistinctState<TState>(init);
	}

	@Override
	public DistinctState<TState> reduce(DistinctState<TState> state, TridentTuple tuple) {
		assert state != null;
		Object paramValue = this._param.eval(tuple);
		boolean distinct = state.filter.addIfNew(paramValue);
		if (distinct == false)
			return state;
		state.accumulated = this._aggObj.accumulate(state.accumulated, paramValue);
		return state;
	}

	@Override
	public DistinctState<TState> localCombinePreFinish(ISizeAwareIterable<DistinctState<TState>> states) {
		if (states.size() == 0) {
			return new DistinctState<TState>(null);
		}
		Iterator<DistinctState<TState>> iter = states.iterator();
		TState state = iter.next().accumulated;
		while(iter.hasNext()) {
			state = this._aggObj.combine(state, iter.next().accumulated);
		}
		return new DistinctState<TState>(state);
	}

	@Override
	public TResult finish(DistinctState<TState> state) {
		return this._aggObj.finish(state.accumulated);
	}

}

final class DistinctState<V> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public final transient DistinctFilter filter = new DistinctFilter();
	public V accumulated;
	
	public DistinctState(V init){
		this.accumulated = init;
	}
	
	@Override
	public String toString(){
		return this.accumulated.toString();
	}
}


