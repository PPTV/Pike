package com.pplive.pike.generator.trident;

import backtype.storm.tuple.Fields;
import java.util.List;
import java.util.Map;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.impl.CaptureCollector;
import storm.trident.operation.impl.ChainedResult;
import storm.trident.tuple.ComboList;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class PikeChainedAggregatorImpl implements Aggregator<PikeChainedResult> {
    Aggregator[] _aggs;
    ProjectionFactory[] _inputFactories;
    ComboList.Factory _fact;
    Fields[] _inputFields;
    
    private final boolean _completeWholeCombination;
    
    public PikeChainedAggregatorImpl(Aggregator[] aggs, Fields[] inputFields, ComboList.Factory fact) {
        this(aggs, inputFields, fact, false);
    }
    
    public PikeChainedAggregatorImpl(Aggregator[] aggs, Fields[] inputFields, ComboList.Factory fact, boolean completeWholeCombination) {
        _aggs = aggs;
        _inputFields = inputFields;
        _fact = fact;
        this._completeWholeCombination = completeWholeCombination;
        if(_aggs.length!=_inputFields.length) {
            throw new IllegalArgumentException("Require input fields for each aggregator");
        }
    }
    
    public void prepare(Map conf, TridentOperationContext context) {
        _inputFactories = new ProjectionFactory[_inputFields.length];
        for(int i=0; i<_inputFields.length; i++) {
            _inputFactories[i] = context.makeProjectionFactory(_inputFields[i]);
            _aggs[i].prepare(conf, new TridentOperationContext(context, _inputFactories[i]));
        }
    }
    
    public PikeChainedResult init(Object batchId, TridentCollector collector) {
    	PikeChainedResult initted = new PikeChainedResult(collector, _aggs.length);
        for(int i=0; i<_aggs.length; i++) {
            initted.objs[i] = _aggs[i].init(batchId, initted.collectors[i]);
        }
        return initted;
    }
    
    public void aggregate(PikeChainedResult val, TridentTuple tuple, TridentCollector collector) {
        val.setFollowThroughCollector(collector);
        for(int i=0; i<_aggs.length; i++) {
            TridentTuple projected = _inputFactories[i].create((TridentTupleView) tuple);
            _aggs[i].aggregate(val.objs[i], projected, val.collectors[i]);
        }
    }
    
    public void complete(PikeChainedResult val, TridentCollector collector) {
        val.setFollowThroughCollector(collector);
        for(int i=0; i<_aggs.length; i++) {
        	if (this._completeWholeCombination == false
        			|| (_aggs[i] instanceof ICombinableAggregator) == false) {
        		_aggs[i].complete(val.objs[i], val.collectors[i]);
        	}
        	else {
           		ICombinableAggregator agg = (ICombinableAggregator)_aggs[i];
           		agg.completeWholeCombination(val.objs[i], val.collectors[i]);
        	}
        }
        if(_aggs.length > 1) { // otherwise, tuples were emitted directly
            int[] indices = new int[val.collectors.length];
            for(int i=0; i<indices.length; i++) {
                indices[i] = 0;
            }
            boolean keepGoing = true;
            //emit cross-join of all emitted tuples
            while(keepGoing) {
                List[] combined = new List[_aggs.length];
                for(int i=0; i< _aggs.length; i++) {
                    CaptureCollector capturer = (CaptureCollector) val.collectors[i];
                    combined[i] = capturer.captured.get(indices[i]);
                }
                collector.emit(_fact.create(combined));
                keepGoing = increment(val.collectors, indices, indices.length - 1);
            }
        }
    }
    
    //return false if can't increment anymore
    private boolean increment(TridentCollector[] lengths, int[] indices, int j) {
        if(j==-1) return false;
        indices[j]++;
        CaptureCollector capturer = (CaptureCollector) lengths[j];
        if(indices[j] >= capturer.captured.size()) {
            indices[j] = 0;
            return increment(lengths, indices, j-1);
        }
        return true;
    }
    
    public void cleanup() {
       for(Aggregator a: _aggs) {
           a.cleanup();
       } 
    } 
}
