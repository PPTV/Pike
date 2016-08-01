package com.pplive.pike.parser;

import com.pplive.pike.base.CaseIgnoredString;

class InternalColumnName
{
	private static long _columnNo;
	
	public static void reset() {
		_columnNo = 0;
	}
	
	public static CaseIgnoredString genColumnName(){
		return genColumnName("");
	}
	
	public static CaseIgnoredString genColumnName(String basedColumnName){
		return genColumnName(new CaseIgnoredString(basedColumnName));
	}
	
	public static CaseIgnoredString genColumnName(CaseIgnoredString basedColumnName){
		_columnNo += 1;
		if (basedColumnName == null || basedColumnName.isEmpty() )
			return new CaseIgnoredString(String.format("<unnamed_%d>", _columnNo));
		else
			return new CaseIgnoredString(String.format("%s_%d", basedColumnName, _columnNo));
	}
}