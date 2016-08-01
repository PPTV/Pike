package com.pplive.pike.exec.output;

import java.io.Closeable;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.base.Period;

public interface IPikeOutput extends Closeable {

	public void init(@SuppressWarnings("rawtypes") Map conf,
			OutputSchema outputSchema, String targetName, Period outputPeriod);


	public void write(Calendar periodEnd, ISizeAwareIterable<List<Object>> tuples);

	public void close() throws IOException;

}
