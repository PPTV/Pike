package storm.trident.operation;

import java.util.*;

import backtype.storm.tuple.Values;

import com.pplive.pike.base.Period;
import com.pplive.pike.generator.trident.ICombinable;


public class AccumulateCombinableAggregator<TState, TResult> extends CombinableAggregator<TState, TResult> {

	private static final long serialVersionUID = 1L;
	
    public AccumulateCombinableAggregator(int id, Period basePeriod, Period aggregatePeriod, ICombinable<TState, TResult> agg, List<String> groupColumns) {
    	super(id, basePeriod, aggregatePeriod, agg, groupColumns);
    }

    public AccumulateCombinableAggregator(AccumulateCombinableAggregator<TState, TResult> other, ICombinable<TState, TResult> agg) {
        super(other, agg);
    }

    public void prepare(Map conf, TridentOperationContext context) {
        this._aggObj.prepare(conf, context);
    }
    
    private void resetAccumulatedState(String groupValuesKey){
        String taskDataKey = getTaskDataKey("$$accumulatedState", groupValuesKey);
		setTaskData(taskDataKey, null);

        Calendar newPeriodEnd = AggregatePeriodHelper.newPeriodEnd(this._basePeriod, this._aggregatePeriod);

        String taskDataKey_AccumulateEnd = getTaskDataKey("$$accumulateEnd", groupValuesKey);
		setTaskData(taskDataKey_AccumulateEnd, newPeriodEnd);
    }
    private void setAccumulatedState(String groupValuesKey, TState val) {
        String taskDataKey = getTaskDataKey("$$accumulatedState", groupValuesKey);
		setTaskData(taskDataKey, val);
    }
    
    private TState getAccumulatedState(String groupValuesKey) {
        String taskDataKey = getTaskDataKey("$$accumulatedState", groupValuesKey);
        Object obj = getTaskData(taskDataKey);
        return (TState)obj;
    }
    
    private Calendar getAggregatePeriodEnd(String groupValuesKey) {
        String taskDataKey_AccumulateEnd = getTaskDataKey("$$accumulateEnd", groupValuesKey);
        Object obj = getTaskData(taskDataKey_AccumulateEnd);
        if (obj == null ){
            obj = AggregatePeriodHelper.currentPeriodEnd(_basePeriod, this._aggregatePeriod);
            setTaskData(taskDataKey_AccumulateEnd, obj);
        }
        return (Calendar)obj;
    }
	
	@Override
	public void completeWholeCombination(GroupState<TState> val, TridentCollector collector) {
		if (this._aggregatePeriodCount <= 1) {
	    	super.completeWholeCombination(val, collector);        
	        return;
		}

        String groupValsKey = groupValuesKey(val.groupValues);
		TState state = getAccumulatedState(groupValsKey);

        Calendar periodEnd = getAggregatePeriodEnd(groupValsKey);
		if (AggregatePeriodHelper.checkAccumulateEnd(periodEnd, _basePeriod)){
            resetAccumulatedState(groupValsKey);
            Calendar curPeriodEnd = AggregatePeriodHelper.currentPeriodEnd(_basePeriod, _aggregatePeriod);
            if (curPeriodEnd.after(periodEnd)) {
                state = val.obj;
                setAccumulatedState(groupValsKey, state);
            }
            else {
                state = (state == null ? val.obj : this._aggObj.combine(state, val.obj));
            }
		}
		else{
            state = (state == null ? val.obj : this._aggObj.combine(state, val.obj));
            setAccumulatedState(groupValsKey, state);
		}
    	
    	TResult result = this._aggObj.finish(state);
        collector.emit(new Values(result));
	}

}
