package com.pplive.pike.parser;

import com.pplive.pike.base.CaseIgnoredString;

class InternalTableName
{
	private static long _tableNo;
	
	public static void reset() {
		_tableNo = 0;
	}
	
	public static CaseIgnoredString genTableName(String basedTableName){
		return genTableName(new CaseIgnoredString(basedTableName));
	}
	
	public static CaseIgnoredString genTableName(CaseIgnoredString basedTableName){
		_tableNo += 1;
		if (basedTableName == null || basedTableName.isEmpty() )
			return new CaseIgnoredString(String.format("table_%d", _tableNo));
		else
			return new CaseIgnoredString(String.format("%s_%d", basedTableName, _tableNo));
	}
}