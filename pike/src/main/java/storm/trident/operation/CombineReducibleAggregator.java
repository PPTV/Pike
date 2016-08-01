package storm.trident.operation;

import java.util.*;

import com.pplive.pike.base.Period;
import storm.trident.tuple.TridentTuple;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

import com.pplive.pike.base.SizeAwareIterable;
import com.pplive.pike.generator.trident.ICombineReducible;

public class CombineReducibleAggregator<TState, TResult>
        extends MultiPeriodAggregator<TState>
        implements Aggregator<GroupState<TState>> {

	private static final long serialVersionUID = 1L;

	protected ICombineReducible<TState, TResult> _aggObj;

    public CombineReducibleAggregator(int id, Period basePeriod, Period aggregatePeriod, ICombineReducible<TState, TResult> agg, List<String> groupColumns) {
        super(id, basePeriod, aggregatePeriod, groupColumns);
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
        val.obj = this._aggObj.reduce(val.obj, tuple);
        if (this._aggregatePeriodCount > 1 && this._groupFields != null && val.groupValues == null) {
            val.groupValues = new LinkedList<Object>();
            for(TupleField f : this._groupFields) {
                val.groupValues.add(f.getValue(tuple));
            }
        }
    }
    
    public void complete(GroupState<TState> val, TridentCollector collector) {
		if (this._aggregatePeriodCount == 1) {
	    	TResult result = this._aggObj.finish(val.obj);
	        collector.emit(new Values(result));        
	        return;
		}
		
		LinkedList<SlidingState<TState>> slidingStates = getSlidingStates(val.groupValues);
        removeExpiredStates(slidingStates);
        slidingStates.addLast(new SlidingState<TState>(currentPeriodEnd(), val.obj));

        List<TState> states = new LinkedList<TState>();
        for(SlidingState<TState> ss : slidingStates) {
            states.add(ss.state);
        }
    	TState mergedState = this._aggObj.localCombinePreFinish(SizeAwareIterable.of(states));
    	TResult result = this._aggObj.finish(mergedState);
        collector.emit(new Values(result));
    }
}
