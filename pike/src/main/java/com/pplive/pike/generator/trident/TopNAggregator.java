package com.pplive.pike.generator.trident;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

import com.pplive.pike.expression.ColumnExpression;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.base.SortOrder;
import com.pplive.pike.base.WrappedObject;
import com.pplive.pike.parser.OrderByColumn;
import com.pplive.pike.util.CollectionUtil;

public class TopNAggregator implements Aggregator<Object> {

	private static final long serialVersionUID = 1L;
	
	private final long _topNumber;
	private final ArrayList<OrderByColumn> _orderByColumns;

    private final ArrayList<ColumnExpression> _topGroupColumns;

    public TopNAggregator(long topNumber, Iterable<OrderByColumn> orderByColumns) {
        this(topNumber, orderByColumns, null);
    }

    public TopNAggregator(long topNumber, Iterable<OrderByColumn> orderByColumns, Iterable<ColumnExpression> topGroupColumns) {
        if (topNumber < 0)
            throw new IllegalArgumentException("topNumber must be >= 0, 0 means no limit");

        this._topNumber = topNumber;
        if (orderByColumns == null){
            this._orderByColumns = new ArrayList<OrderByColumn>(0);
        }
        else{
            this._orderByColumns = CollectionUtil.copyArrayList(orderByColumns);
        }

        if (topGroupColumns == null){
            this._topGroupColumns = new ArrayList<ColumnExpression>(0);
        }
        else{
            this._topGroupColumns = CollectionUtil.copyArrayList(topGroupColumns);
        }

    }

    public void prepare(Map conf, TridentOperationContext context) {
    }
    
    public void cleanup() {
    }
    
    private boolean noOrderColumn(){
    	return this._orderByColumns.size() == 0;
    }

    private boolean noTopLimit(){
        return this._topNumber == 0;
    }

    public Object init(Object batchId, TridentCollector collector) {
    	if (noOrderColumn()){
            return new WrappedObject<Long>(0L);
    	}
    	
    	int initCapacity = noTopLimit() ? 100 : (int)this._topNumber + 2;
    	return new PriorityQueue<QueueElement>(initCapacity, QueueElementComparator.Instance);
    }
    
    public void aggregate(Object val, TridentTuple tuple, TridentCollector collector) {
    	if (noOrderColumn()){
			@SuppressWarnings("unchecked") WrappedObject<Long> wrappedLong = (WrappedObject<Long>)val;
    		Long emitted = wrappedLong.obj;
    		if (noTopLimit() || emitted < this._topNumber) {
    			wrappedLong.obj = emitted + 1;
        		collector.emit(tuple);
    		}
            return;
    	}
    	
    	PriorityQueue<QueueElement> queue = (PriorityQueue<QueueElement>)val;
    	assert queue != null;
    	
    	OrderByValue[] orderValues = new OrderByValue[this._orderByColumns.size()];
    	for(int n = 0; n < orderValues.length; n += 1){
    		OrderByColumn col = this._orderByColumns.get(n);
    		Object o = col.columnExpr().eval(tuple);
    		assert o instanceof Comparable<?>;
    		orderValues[n] = new OrderByValue((Comparable<?>)o, col.getSortOrder());
    	}
    	
    	QueueElement e = new QueueElement(orderValues, tuple);
        if (noTopLimit() || queue.size() < this._topNumber) {
            queue.add(e);
            return;
        }

        if (e.compareTo(queue.peek()) >= 0) {
            return;
        }
    	queue.add(e);
   		while(queue.size() > this._topNumber){
   			queue.poll();
   		}
    }

    public void complete(Object val, TridentCollector collector) {
    	if (noOrderColumn()){
            return;
    	}
    	
    	@SuppressWarnings("unchecked") PriorityQueue<QueueElement> queue = (PriorityQueue<QueueElement>)val;
    	assert queue != null;
    	Object[] items = queue.toArray();
    	Arrays.sort(items);
    	for(Object o : items) {
    		collector.emit(((QueueElement)o).Tuple);
    	}
    }
    
    static class OrderByValue {
    	public final Comparable Value;
    	public final SortOrder Order;
    	
    	public OrderByValue(Comparable<?> val, SortOrder order){
    		this.Value = val;
    		this.Order = order;
    	}

        @Override
        public String toString() {
            return String.format("%s", this.Value);
        }
    }
    
    // PriorityQueue is a min-root heap, so we need inverse the compare result
    // because we want a max-root heap
    static class QueueElementComparator implements Comparator<QueueElement>, java.io.Serializable {
		private static final long serialVersionUID = 1L;

        public static final QueueElementComparator Instance = new QueueElementComparator();

		public int compare(QueueElement left, QueueElement right) {
        	if (left == null || right == null)
        		throw new NullPointerException();
        	return -left.compareTo(right);
        }
    }
    
    static class QueueElement implements Comparable<QueueElement> {
    	private final OrderByValue[] _orderValues;
    	public final TridentTuple Tuple;
    	
    	public QueueElement(OrderByValue[] values, TridentTuple tuple){
    		assert values != null;
    		this._orderValues = values;
    		this.Tuple = tuple;
    	}
    	
    	@Override
    	public boolean equals(Object o){
    		if (o == null || o.getClass() != this.getClass())
    			return false;
    		return equals((QueueElement)o);
    	}
    	
    	public boolean equals(QueueElement other) {
    		if (this._orderValues.length != other._orderValues.length)
    			return false;
    		return compareTo(other) == 0;
    	}

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(this._orderValues[0]);
            for(int n = 1; n < this._orderValues.length; n +=1){
                sb.append(", ").append(this._orderValues[n]);
            }
            sb.append("]");
            return sb.toString();
        }
    	
    	@Override
    	public int hashCode(){
    		return HashFailThrower.throwOnHash(this);
    	}
    	
    	public int compareTo(QueueElement other){
    		if (other == null) {
    			assert false;
    			throw new NullPointerException();
    		}
    		if (this._orderValues.length != other._orderValues.length){
				assert false;
    			throw new IllegalStateException();
    		}
    		
    		for(int n = 0; n < this._orderValues.length; n += 1){
    			OrderByValue left = this._orderValues[n];
    			OrderByValue right = other._orderValues[n];
    			assert left != null && right != null;
    			assert left.Order != null && right.Order != null;
    			
    			if (left.Order != right.Order) {
    				assert false;
        			throw new IllegalStateException();
    			}
    			
    			if (left.Value == null)
    				return left.Order == SortOrder.Descending ? 1 : -1;
    			if (right.Value == null)
    				return left.Order == SortOrder.Descending ? -1 : 1;
    			
    			assert left.Value.getClass() == right.Value.getClass();
    			
    			@SuppressWarnings("unchecked") int res = left.Value.compareTo(right.Value);
    			if(left.Order == SortOrder.Descending){
    				res = -res;
    			}
    			if (res != 0)
    				return res;
    		}
    		return 0;
    	}
    }
}
