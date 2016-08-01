package storm.trident.operation;

import com.pplive.pike.base.Period;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MultiPeriodAggregator<TState> implements Serializable {

    protected final int _id; // unique id in topology;
    protected final Period _basePeriod;
    public Period getBasePeriod() { return _basePeriod; }
    protected final Period _aggregatePeriod;
    public Period getAggregatePeriod() { return _aggregatePeriod; }
    protected final int _aggregatePeriodCount;
    protected final List<TupleField> _groupFields;

    public List<String> getGroupColumns() {
        if (_groupFields == null)
            return new LinkedList<String>();
        return fieldsToCols(_groupFields);
    }

    protected MultiPeriodAggregator(int id, Period basePeriod, Period aggregatePeriod, List<String> groupColumns) {
        this._id = id;
        this._basePeriod = basePeriod;
        this._aggregatePeriod = aggregatePeriod;
        this._aggregatePeriodCount = aggregatePeriod.periodSeconds() / basePeriod.periodSeconds();
        if (groupColumns == null || groupColumns.isEmpty()) {
            this._groupFields = null;
        }
        else {
            this._groupFields = new LinkedList<TupleField>();
            for(String s : groupColumns) {
                this._groupFields.add(new TupleField(s));
            }
        }
    }

    private static List<String> fieldsToCols(List<TupleField> groupFields) {
        if (groupFields == null)
            return null;
        List<String> cols = new LinkedList<String>();
        for(TupleField f : groupFields) {
            cols.add(f.getColumnName());
        }
        return cols;
    }

    private static int _nextId = 100000;
    public static int nextId() {
        _nextId += 100000;
        return _nextId;
    }

    protected MultiPeriodAggregator(MultiPeriodAggregator other) {
        this(nextId() + other._id, other._basePeriod, other._aggregatePeriod, fieldsToCols(other._groupFields));
    }

    protected static String groupValuesKey(List<Object> groupValues) {
        if (groupValues == null) {
            return "";
        }
        String s = "";
        for(Object o : groupValues) {
            s += (o == null ? "_<NULL>" : "_" + o);
        }
        return s;
    }

    private final static Map<String, Object> _taskData = new ConcurrentHashMap<String, Object>();

    protected void setTaskData(String name, Object data) {
        if (data == null) {
            _taskData.remove(name);
        }
        else {
            _taskData.put(name, data);
        }
    }

    protected Object getTaskData(String name) {
        return _taskData.get(name);
    }

    protected String getTaskDataKey(String keyPrefix, List<Object> groupValues) {
        return getTaskDataKey(keyPrefix, groupValuesKey(groupValues));
    }

    protected String getTaskDataKey(String keyPrefix, String groupValuesKey) {
        String s = String.format("%s-%d-%s", keyPrefix, this._id, groupValuesKey);
        return s;
    }

    protected LinkedList<SlidingState<TState>> getSlidingStates(List<Object> groupValues) {
        assert this._aggregatePeriodCount > 1;

        String taskDataKey = getTaskDataKey("$$periodStates", groupValues);
        Object obj = getTaskData(taskDataKey);
        if (obj == null) {
            obj = new LinkedList<SlidingState<TState>>();
            setTaskData(taskDataKey, obj);
        }
        assert obj instanceof LinkedList<?>;
        return (LinkedList<SlidingState<TState>>)obj;
    }

    protected void removeExpiredStates(LinkedList<SlidingState<TState>> slidingStates) {
        Calendar t = currentPeriodEnd();
        t.add(Calendar.SECOND, - _basePeriod.periodSeconds() * _aggregatePeriodCount);

        while(slidingStates.size() > this._aggregatePeriodCount) {
            slidingStates.removeFirst();
        }
        while (not(slidingStates.isEmpty())) {
            SlidingState<TState> ss = slidingStates.getFirst();
            if (ss.periodEnd.after(t))
                break;
            slidingStates.removeFirst();
        }
    }

    private static boolean not(boolean expr) { return !expr; }

    protected Calendar currentPeriodEnd() {
        int delayMs = decideTolerantDelayMilliseconds(_basePeriod);
        return _basePeriod.currentPeriodEnd(delayMs);
    }

    private static int decideTolerantDelayMilliseconds(Period period) {
        return decideTolerantDelayMilliseconds(period.periodSeconds());
    }

    private static int decideTolerantDelayMilliseconds(int periodSeconds) {
        int seconds = periodSeconds;
        if (seconds <= 6){
            if (seconds <= 1)
                return 500;
            else if (seconds <= 3)
                return 1000;
            else
                return 2000;
        }
        else {
            if (seconds <= 20)
                return 3000;
            else if (seconds <= 180)
                return 5000;
            else
                return 10000;
        }
    }
}
