package com.pplive.pike.exec.output;

import java.io.Serializable;

import com.pplive.pike.base.Immutable;
import com.pplive.pike.base.Period;

@Immutable
public class OutputTarget implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private final OutputType _type;
	public OutputType getType(){
		return this._type;
	}
	
	private final String _targetName;
	public String getTargetName(){
		return this._targetName;
	}

    private final Period _outputPeriod;
    public Period getOutputPeriod(){
        return this._outputPeriod;
    }

	public OutputTarget(OutputType type, String targetName, Period period){
		this._type = type;
        this._targetName = targetName;
        this._outputPeriod = period;
	}
	
	@Override
	public String toString() {
		return String.format("%s.%s", this._type, this._targetName);
	}
}
