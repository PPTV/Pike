package com.pplive.pike.generator.trident;

import java.util.Map;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import backtype.storm.tuple.Values;

import com.pplive.pike.base.WrappedObject;

public class ReducibleAggregator<TState, TResult> implements Aggregator<WrappedObject<TState>> {

	private static final long serialVersionUID = 1L;
	
	private IReducible<TState, TResult> _aggObj;

    public ReducibleAggregator(IReducible<TState, TResult> agg) {
        this._aggObj = agg;
    }
    
    public void prepare(Map conf, TridentOperationContext context) {
        this._aggObj.prepare(conf, context);
    }
    
    public void cleanup() {
        this._aggObj.cleanup();
    }
    
    public WrappedObject<TState> init(Object batchId, TridentCollector collector) {
        TState state = this._aggObj.init();
        return new WrappedObject<TState>(state);
    }
    
    public void aggregate(WrappedObject<TState> val, TridentTuple tuple, TridentCollector collector) {
        val.obj = this._aggObj.reduce(val.obj, tuple);
    }
    
    public void complete(WrappedObject<TState> val, TridentCollector collector) {
    	TResult result = this._aggObj.finish(val.obj);
        collector.emit(new Values(result));        
    }
}
