package com.pplive.pike.function.builtin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.generator.trident.ICombinable;
import com.pplive.pike.generator.trident.ICombineReducible;

import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class Count implements Serializable {
		
	private static final long serialVersionUID = 5130012702557953161L;

	public static ICombinable<CountState, Long> createCombinable(List<AbstractExpression> params) {
		if (params == null){
			throw new IllegalArgumentException("params cannot be null");
		}
		return new CountCombiner(params);
	}

	public static ICombineReducible<?, ?> createDistinctReducible(AbstractExpression param) {
		if (param == null){
			throw new IllegalArgumentException("param cannot be null");
		}
		return new CountDistinctReducer(param);
	}
	
	public static ICombineReducible<?, ?> createDistinctReducible(List<AbstractExpression> params) {
		if (params == null){
			throw new IllegalArgumentException("params cannot be null");
		}
		if (params.size() == 1) {
			return createDistinctReducible(params.get(0));
		}
		else {
			return new CountDistinctReducerN(params);
		}
	}
	
}

class CountCombiner implements ICombinable<CountState, Long> {
	
	private static final long serialVersionUID = -166056224783723673L;
	
	private final ArrayList<AbstractExpression> _params;

	public CountCombiner(List<AbstractExpression> params) {
		assert params != null && params.size() > 0;

		this._params = new ArrayList<AbstractExpression>(params);
	}

    @Override
    public ICombinable<CountState, Long> cloneForStateCombine(AbstractExpression stateExpr) {
        return new CountCombiner(Arrays.asList(stateExpr));
    }

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public CountState evalToCombinable(TridentTuple tuple) {
        Object obj = this._params.get(0).eval(tuple);
        if (obj == null) {
            return CountState.Zero;
        }
        if (obj instanceof CountState) {
            return (CountState)obj;
        }
		return CountState.One;
	}

	@Override
	public CountState init() {
		return CountState.Zero;
	}
	
	@Override
    public CountState combine(CountState left, CountState right) {
		if (left == null) 
			return right;
		if (right == null)
			return left;
		return new CountState(left.count() + right.count());
	}

	@Override
	public Long finish(CountState val) {
		return val != null ? val.count() : 0L;
	}
}

final class CountState implements Serializable {
    private final long _count;
    public long count() { return _count; }
    public CountState(long count) {
        this._count = count;
    }

    public static final CountState Zero = new CountState(0);
    public static final CountState One = new CountState(1);

    @Override
    public String toString() {
        return String.format("%d", _count);
    }
}

class CountDistinctReducer implements ICombineReducible<DistinctFilter, Long> {
	
	private static final long serialVersionUID = 1L;
	
	private final AbstractExpression _param;
	
	public CountDistinctReducer(AbstractExpression param) {
		assert param != null;

		this._param = param;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}
	
	@Override
	public DistinctFilter init() {
		return new DistinctFilter();
	}
		
	@Override
    public DistinctFilter reduce(DistinctFilter filter, TridentTuple tuple) {
		assert filter != null;
		Object paramValue = this._param.eval(tuple);
		filter.addIfNew(paramValue);
		return filter;
    }

	@Override
	public DistinctFilter localCombinePreFinish(ISizeAwareIterable<DistinctFilter> distinctFilters) {
		return DistinctFilter.merge(distinctFilters);
	}

	@Override
	public Long finish(DistinctFilter filter) {
		return filter.size();
	}
}

class CountDistinctReducerN implements ICombineReducible<DistinctFilterN, Long> {
	
	private static final long serialVersionUID = -368398248817675028L;
	
	private final ArrayList<AbstractExpression> _params;
	
	public CountDistinctReducerN(List<AbstractExpression> params) {
		assert params != null && params.size() > 0;

		this._params = new ArrayList<AbstractExpression>(params);
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}
	
	@Override
	public DistinctFilterN init() {
		return new DistinctFilterN();
	}
		
	@Override
    public DistinctFilterN reduce(DistinctFilterN filter, TridentTuple tuple) {
		assert filter != null;
		
		Object[] paramValues = new Object[this._params.size()];

		for (int i = 0; i < this._params.size(); i += 1) {
			paramValues[i] = this._params.get(i).eval(tuple);
		}
		
		filter.addIfNew(paramValues);
    	return filter;
    }

	@Override
	public DistinctFilterN localCombinePreFinish(ISizeAwareIterable<DistinctFilterN> states) {
		return DistinctFilterN.merge(states);	}

	@Override
	public Long finish(DistinctFilterN filter) {
		return filter.size();
	}

}
