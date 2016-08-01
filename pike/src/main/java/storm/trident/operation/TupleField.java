package storm.trident.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleViewHelper;

import java.io.Serializable;
import java.util.List;

class TupleField  implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String columnName;
	public String getColumnName() { return columnName; }

	private int _cachedColumnIndex = -1;

	public TupleField(String columnName){
		this.columnName = columnName;
	}

	public Object getValue(TridentTuple tuple) {
		if (this._cachedColumnIndex >= 0) {
			assert this._cachedColumnIndex < tuple.size();
			if (this._cachedColumnIndex < tuple.size())
				return tuple.getValue(this._cachedColumnIndex);
			// todo: need log warning, if come here something must be wrong 
		}
		
		this._cachedColumnIndex = findIndex(tuple, this.columnName);
		if (this._cachedColumnIndex < 0){
			return null;
		}
		
		Object o = tuple.getValue(this._cachedColumnIndex);
		return o;
	}

	@Override
	public String toString(){
		return this.columnName;
	}

    static int findIndex(TridentTuple tuple, String field) {
        if (tuple instanceof TridentTupleView) {
            return TridentTupleViewHelper.findFieldIndex((TridentTupleView) tuple, field);
        }

        // Warning: since TridentTuple has no method to get field index,
        // we have to use tricky way, it's possible get incorrect result,
        // if tuple has two fields reference to same object.
        Object obj = tuple.getValueByField(field);

        if (obj == null)
            return -1;
        List<Object> objs = tuple.getValues();
        int n = 0;
        for(Object o : objs){
            if (o == obj)
                return n;
            n += 1;
        }
        assert false;
        return -1;
    }
}
