package com.pplive.pike.function;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.function.builtin.Join;

public class MemoryJoinTest extends BaseUnitTest {
	// @Test
	public void channel() {

		Assert.assertEquals(StringUtils.join(Join.MemoryTransJoin.evaluate(102,
				"dim_sync_cdn_node", "TitleChinese")), "221.192.146.134");
		Assert.assertEquals(StringUtils.join(Join.MemoryTransJoin.evaluate(102,
				"dim_sync_cdn_node", "SequenceId", "TitleChinese",
				"ServerGroup"), ","), "221.192.146.134,200");

		Assert.assertEquals(
				this.showTwoArray(Join.MemoryTransJoin.evaluate(102, false,
						"dim_sync_cdn_node", "ServerGroup", "TitleChinese")),
				"61.158.254.131|61.158.254.132|61.158.254.133|61.158.254.135|61.158.254.136|61.158.254.138|61.158.254.139|61.158.254.140|61.158.254.141|61.158.254.143|61.158.254.145|61.158.254.147|61.158.254.148|61.158.254.23|61.158.254.24|61.158.254.25|61.158.254.26|61.158.254.27|61.158.254.153|61.158.254.155|61.158.254.12|61.158.254.13|61.158.254.14|61.158.254.15|61.158.254.16|61.158.254.17|61.158.254.18|61.158.254.19|61.158.254.20|61.158.254.21|61.158.254.134|61.158.254.146|61.158.254.142|61.158.254.159|61.158.254.28|61.158.254.29|182.118.24.16|182.118.24.17|182.118.24.18|182.118.24.19|182.118.24.20|182.118.24.21|182.118.24.22|182.118.24.23|182.118.24.24|182.118.24.25|182.118.24.26|182.118.24.15");

		Assert.assertEquals(
				this.showTwoArray(Join.MemoryTransJoin.evaluate(102, false,
						"dim_sync_cdn_node", "ServerGroup", "TitleChinese",
						"ext_id")),
				"61.158.254.131,120|61.158.254.132,121|61.158.254.133,124|61.158.254.135,125|61.158.254.136,126|61.158.254.138,128|61.158.254.139,129|61.158.254.140,130|61.158.254.141,131|61.158.254.143,133|61.158.254.145,135|61.158.254.147,137|61.158.254.148,138|61.158.254.23,139|61.158.254.24,140|61.158.254.25,141|61.158.254.26,142|61.158.254.27,143|61.158.254.153,331|61.158.254.155,333|61.158.254.12,379|61.158.254.13,380|61.158.254.14,381|61.158.254.15,382|61.158.254.16,383|61.158.254.17,384|61.158.254.18,385|61.158.254.19,386|61.158.254.20,387|61.158.254.21,388|61.158.254.134,519|61.158.254.146,534|61.158.254.142,552|61.158.254.159,625|61.158.254.28,640|61.158.254.29,641|182.118.24.16,3209|182.118.24.17,3210|182.118.24.18,3211|182.118.24.19,3212|182.118.24.20,3213|182.118.24.21,3214|182.118.24.22,3215|182.118.24.23,3216|182.118.24.24,3217|182.118.24.25,3218|182.118.24.26,3219|182.118.24.15,3220");

		Assert.assertEquals(StringUtils.join(Join.MemoryTransJoin.evaluate(103,
				"dim_sync_channel", "SeriesId")), "10058429");

		Assert.assertEquals(StringUtils.join(Join.MemoryTransJoin.evaluate(103,
				"dim_sync_channel", "SequenceId", "SeriesId", "Dim_Copyright"),
				","), "10058429,2");

		Assert.assertEquals(StringUtils.join(Join.MemoryTransJoin.evaluate(103,
				"dim_sync_channel", "SequenceId", "SeriesId", "Dim_Copyright",
				"Channel_TitleChinese"), ","), "10058429,2,金钱本色(第06集)");
		Assert.assertEquals(
				this.showTwoArray(Join.MemoryTransJoin.evaluate(10020405,
						false, "dim_sync_channel", "SeriesId",
						"Channel_TitleChinese")),
				"战地英雄(第01集)|战地英雄(第05集)|战地英雄(第03集)|战地英雄(第09集)|战地英雄(第10集)|战地英雄(第12集)|战地英雄(第15集)|战地英雄(第20集)|战地英雄(第02集)|战地英雄(第11集)|战地英雄(第04集)|战地英雄(第06集)|战地英雄(第07集)|战地英雄(第08集)|战地英雄(第13集)|战地英雄(第14集)|战地英雄(第16集)|战地英雄(第17集)|战地英雄(第19集)|战地英雄(第18集)|战地英雄");
		Assert.assertEquals(
				this.showTwoArray(Join.MemoryTransJoin.evaluate(10020405,
						false, "dim_sync_channel", "SeriesId", "AlternateKey",
						"Channel_TitleChinese")),
				"10071963,战地英雄(第01集)|10071967,战地英雄(第05集)|10071965,战地英雄(第03集)|10071971,战地英雄(第09集)|10071972,战地英雄(第10集)"
						+ "|10071974,战地英雄(第12集)|10071977,战地英雄(第15集)|10071982,战地英雄(第20集)|10071964,战地英雄(第02集)"
						+ "|10071973,战地英雄(第11集)|10071966,战地英雄(第04集)|10071968,战地英雄(第06集)|10071969,战地英雄(第07集)"
						+ "|10071970,战地英雄(第08集)|10071975,战地英雄(第13集)|10071976,战地英雄(第14集)|10071978,战地英雄(第16集)"
						+ "|10071979,战地英雄(第17集)|10071981,战地英雄(第19集)|10071980,战地英雄(第18集)|10020405,战地英雄");

	}

	private String showTwoArray(String[][] array) {
		String[] vs = new String[array.length];
		for (int i = 0; i < array.length; i++) {
			vs[i] = StringUtils.join(array[i], ",");
		}
		return StringUtils.join(vs, "|");
	}
}
