package com.pplive.pike.exec.output;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import com.pplive.pike.base.Immutable;
import com.pplive.pike.base.Period;
import com.pplive.pike.util.CollectionUtil;

@Immutable
public class OutputSchema implements Serializable {

	private static final long serialVersionUID = -6497119056244427566L;

	private final ArrayList<OutputField> _outputFields;
	private final String _topologyName;

	public Iterable<OutputField> getOutputFields() {
		return this._outputFields;
	}

	public int getOutputFieldCount() {
		return this._outputFields.size();
	}

	public String getTopologyName() {
		return this._topologyName;
	}

	public OutputSchema(String topologyName,
			Iterable<OutputField> outputFields) {
		this._topologyName = topologyName;
		this._outputFields = CollectionUtil.copyArrayList(outputFields);
	}
	public OutputField getOutputField(int index) {
		return this._outputFields.get(index);
	}

	public int indexOfColumn(String columnName) {
		int index = 0;
		for (OutputField f : this._outputFields) {
			if (f.getName().equalsIgnoreCase(columnName))
				return index;
			index++;
		}
		return -1;
	}
}
