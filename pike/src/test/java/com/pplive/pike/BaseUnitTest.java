package com.pplive.pike;

import org.apache.commons.lang.StringUtils;
import org.junit.Assume;


public class BaseUnitTest {
	/**
	 * AppService依赖，包括thrift
	 */
	public final static int AppServerDependent = 8;
	/**
	 * 本地文件依赖，
	 */
	public final static int LocalFileDependent = 16;

	// public final static

	public void assumeScope(int dependents) {
		String value = System.getenv("BIP_UNIT_TEST");
		if (StringUtils.isEmpty(value)) {
			value = "0";
		}
		if (!"all".equalsIgnoreCase(value)) {
			int settingScope = Integer.parseInt(value);
			Assume.assumeTrue((dependents & settingScope) == dependents);
		}
	}
}
