package com.pplive.pike.exec.output;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;


import com.pplive.pike.Configuration;
import com.pplive.pike.base.Period;
import com.pplive.pike.base.SizeAwareIterable;
import com.pplive.pike.exec.spoutproto.ColumnType;
@Ignore
public class SQLServerBulkOutputTest {
	
	@Test
	public void insert() throws java.io.IOException {
		SQLServerBulkOutput output = new SQLServerBulkOutput();
		List<OutputField> fields = new ArrayList<OutputField>();
		fields.add(new OutputField("dt", ColumnType.Long));
		fields.add(new OutputField("Boolean", ColumnType.Boolean));
		fields.add(new OutputField("Byte", ColumnType.Byte));
		fields.add(new OutputField("Date", ColumnType.Date));
		fields.add(new OutputField("Double", ColumnType.Double));
		fields.add(new OutputField("Float", ColumnType.Float));
		fields.add(new OutputField("Map_ObjString", ColumnType.Map_ObjString));
		fields.add(new OutputField("Short", ColumnType.Short));
		fields.add(new OutputField("String", ColumnType.String));
		fields.add(new OutputField("Time", ColumnType.Time));
		fields.add(new OutputField("Timestamp", ColumnType.Timestamp));
		fields.add(new OutputField("Unknown", ColumnType.Unknown));

		OutputSchema schema = new OutputSchema("topologyName", fields);
		output.init(new Configuration(), schema, "diy.test", Period.secondsOf(300));
		List<List<Object>> values = new ArrayList<List<Object>>();
		for (int i = 0; i < 100; i++) {
			List<Object> value = new ArrayList<Object>();
			value.add("10");
			value.add(new Boolean(false));
			value.add(new Byte("2"));
			value.add(new Date());
			value.add((new Random()).nextDouble());
			value.add((new Random()).nextFloat());
			value.add("map");
			value.add(Short.MAX_VALUE);
			value.add("string");
			value.add(new Date());
			value.add(new Date());
			value.add(new Date());
			values.add(value);
		}

		output.write(Calendar.getInstance(), SizeAwareIterable.of(values));

		// SizeAwareIterable<List<Object>> values = SizeAwareIterable.of(coll);
	}
}
