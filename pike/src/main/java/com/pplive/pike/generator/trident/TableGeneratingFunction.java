package com.pplive.pike.generator.trident;

import com.pplive.pike.expression.FunctionExpression;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TableGeneratingFunction extends BaseFunction {

	private static final long serialVersionUID = -8198792520720173149L;

    private FunctionExpression _udtfExpr;
    private int _returnFieldCount;

	public TableGeneratingFunction(FunctionExpression udtfExpr, int returnFieldCount) {
        assert udtfExpr != null;
        assert returnFieldCount > 0;

        this._udtfExpr = udtfExpr;
        this._returnFieldCount = returnFieldCount;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {
        this._udtfExpr.init();
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
        Object res = this._udtfExpr.eval(tuple);
        assert res instanceof Object[] || res instanceof Iterable;

        Iterable<Object> items = getIterable(res);
        if (items == null) {
            List<Object> newTuple = new ArrayList<Object>(this._returnFieldCount);
            for(int n = 0; n < this._returnFieldCount; n += 1) {
                newTuple.add(null);
            }
            collector.emit(newTuple);
            return;
        }

        for (Object item : items) {
            List<Object> newTuple = new ArrayList<Object>(this._returnFieldCount);
            Iterable<Object> fields = getIterable(item);
            int n = 0;
            if (fields != null) {
                for (Object o : fields) {
                    n += 1;
                    if (n > this._returnFieldCount)
                        break;
                    newTuple.add(o);
                }
            }
            while(n < this._returnFieldCount) {
                newTuple.add(null);
                n += 1;
            }
            collector.emit(newTuple);
        }
	}

    private static Iterable<Object> getIterable(Object obj) {
        if (obj == null) {
            return null;
        }
        else if (obj instanceof Object[]) {
            return Arrays.asList((Object[])obj);
        }
        else if (obj instanceof Iterable) {
            return (Iterable<Object>)obj;
        }
        else {
            return Arrays.asList(obj);
        }
    }

	@Override
	public void cleanup() {
	}

}
