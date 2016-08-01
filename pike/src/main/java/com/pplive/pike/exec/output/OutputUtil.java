package com.pplive.pike.exec.output;

import java.text.SimpleDateFormat;
import java.util.Date;

class OutputUtil {

	public static String generateTimeField() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmss");
		return sdf.format(new Date());
	}
	
	private OutputUtil(){}
}
