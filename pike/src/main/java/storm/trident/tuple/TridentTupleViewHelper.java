package storm.trident.tuple;

public class TridentTupleViewHelper {

	public static int findFieldIndex(TridentTupleView tuple, String field) {
		ValuePointer ptr = tuple._fieldIndex.get(field);
		if (ptr == null)
			return -1;
		for(int n = 0; n < tuple._index.length; n +=1){
			if (ptr == tuple._index[n])
				return n;
		}
		return -1;
	}
}
