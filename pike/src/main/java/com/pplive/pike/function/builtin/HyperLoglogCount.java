package com.pplive.pike.function.builtin;

import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.generator.trident.ICombinable;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by jiatingjin on 2017/10/23.
 */
public class HyperLoglogCount {
    public static ICombinable<HyperLoglogCountState, Long> createCombinable(int k, List<AbstractExpression> params) {
        if (params == null){
            throw new IllegalArgumentException("params cannot be null");
        }
        return new HyperLoglogCountCombiner(k, params);
    }
}

class HyperLoglogCountCombiner implements ICombinable<HyperLoglogCountState, Long> {
    private static final long serialVersionUID = 1L;

    private final int _k;
    private final ArrayList<AbstractExpression> _params;

    public HyperLoglogCountCombiner(int k, List<AbstractExpression> params) {
        assert k > 0;
        assert params != null && params.size() > 0;

        this._k = k;
        this._params = new ArrayList<AbstractExpression>(params);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public ICombinable<HyperLoglogCountState, Long> cloneForStateCombine(AbstractExpression stateExpr) {
        return new HyperLoglogCountCombiner(this._k, Arrays.asList(stateExpr));
    }

    @Override
    public HyperLoglogCountState evalToCombinable(TridentTuple tuple) {
        Object[] paramValues = new Object[this._params.size()];

        for (int i = 0; i < this._params.size(); i += 1) {
            paramValues[i] = this._params.get(i).eval(tuple);
        }

        if (paramValues.length == 1 && paramValues[0] instanceof HyperLoglogCountState) {
            System.out.println("evaltocom " + ((HyperLoglogCountState)paramValues[0]).cardinality());
            return (HyperLoglogCountState)paramValues[0];
        }
        return new HyperLoglogCountState(paramValues);
    }

    @Override
    public HyperLoglogCountState init() {
        HyperLogLog c = new HyperLogLog(this._k);
        return new HyperLoglogCountState(new SerializableHyperLoglogCounting(c));
    }

    @Override
    public HyperLoglogCountState combine(HyperLoglogCountState left, HyperLoglogCountState right) {
        if (left == null)
            return right;
        if (right == null)
            return left;
        HyperLoglogCountState state = HyperLoglogCountState.combineNonNull(left, right);
        return state;
    }

    @Override
    public Long finish(HyperLoglogCountState val) {
        if (val == null)
            return null;
        return val.cardinality();
    }
}