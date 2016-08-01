package com.pplive.pike.function;

import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Test;

import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.function.builtin.MapGet;

public class FunctionTest  extends BaseUnitTest {

	@Test
	public void testMapGet() {
		Map<String, Integer> map = new TreeMap<String, Integer>();
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		Assert.assertNull(new MapGet().evaluate(null, "0"));
		Assert.assertNull(new MapGet().evaluate(map, null));
		Assert.assertNull(new MapGet().evaluate(map, ""));
		Assert.assertNull(new MapGet().evaluate(map, "00"));
		Assert.assertEquals(new MapGet().evaluate(map, "0"), "0");
		Assert.assertEquals(new MapGet().evaluate(map, "1"), "1");
		Assert.assertEquals(new MapGet().evaluate(map, "2"), "2");
	}
}
