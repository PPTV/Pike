package storm.trident.operation;

import java.util.*;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

import com.pplive.pike.base.Period;
import com.pplive.pike.generator.trident.ICombinable;
import com.pplive.pike.generator.trident.ICombinableAggregator;

import storm.trident.tuple.TridentTuple;

public class CombinableAggregator<TState, TResult>
        extends MultiPeriodAggregator<TState>
        implements ICombinableAggregator<GroupState<TState>> {

	private static final long serialVersionUID = 1L;

	protected ICombinable<TState, TResult> _aggObj;
    public ICombinable<TState, TResult> getAggObj() { return _aggObj; }

    public CombinableAggregator(int id, Period basePeriod, Period aggregatePeriod, ICombinable<TState, TResult> agg, List<String> groupColumns) {
        super(id, basePeriod, aggregatePeriod, groupColumns);
        this._aggObj = agg;
    }

    public CombinableAggregator(CombinableAggregator<TState, TResult> other, ICombinable<TState, TResult> agg) {
        super(other);
        this._aggObj = agg;
    }

    public void prepare(Map conf, TridentOperationContext context) {
        this._aggObj.prepare(conf, context);
    }

    public void cleanup() {
        this._aggObj.cleanup();
    }
    
    public GroupState<TState> init(Object batchId, TridentCollector collector) {
    	TState state = this._aggObj.init();
        return new GroupState<TState>(state);
    }
    
    public void aggregate(GroupState<TState> val, TridentTuple tuple, TridentCollector collector) {
        TState v = this._aggObj.evalToCombinable(tuple);
        val.obj = this._aggObj.combine(val.obj, v);
        if (this._aggregatePeriodCount > 1 && this._groupFields != null && val.groupValues == null) {
            val.groupValues = new LinkedList<Object>();
            for(TupleField f : this._groupFields) {
                val.groupValues.add(f.getValue(tuple));
            }
        }
    }
    
    public void complete(GroupState<TState> val, TridentCollector collector) {
        collector.emit(new Values(val.obj));
    }

	@Override
	public void completeWholeCombination(GroupState<TState> val, TridentCollector collector) {
		if (this._aggregatePeriodCount <= 1) {
	    	TResult result = this._aggObj.finish(val.obj);
	        collector.emit(new Values(result));        
	        return;
		}
		
		LinkedList<SlidingState<TState>> slidingStates = getSlidingStates(val.groupValues);
        removeExpiredStates(slidingStates);
        slidingStates.addLast(new SlidingState<TState>(currentPeriodEnd(), val.obj));

    	Iterator<SlidingState<TState>> iter = slidingStates.iterator();
        TState mergedState = iter.next().state;
    	while(iter.hasNext()){
    		mergedState = this._aggObj.combine(mergedState, iter.next().state);
    	}
    	TResult result = this._aggObj.finish(mergedState);
        collector.emit(new Values(result));
	}
}
