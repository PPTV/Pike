package storm.trident.operation;

import java.util.List;

class GroupState<T> {
	public T obj;
	public GroupState(T o){
		this.obj = o;
	}

    public List<Object> groupValues;

    @Override
    public String toString() {
        return "GroupState: " + this.obj;
    }
}
