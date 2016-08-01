package com.pplive.pike;

import java.util.List;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.Assert;

import com.pplive.pike.automation.Pike;
import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.parser.LogicalQueryPlan;
import com.pplive.pike.util.ReflectionUtils;

public class PikeUtilsTest  extends BaseUnitTest {

	@Test
    public void parsePlay(){
        LogicalQueryPlan plan = Pike.parseSQL("withperiod 5m select output(dt(5*60)) as dt,plt,case when dim_liveondemand_c=102 then '点播' else '直播' end as liveondemand1,count(distinct userid) as uv,count(userid) as idcount from dol_smart where (dim_liveondemand_c=103 or dim_liveondemand_c=102)  group by plt,liveondemand1",
                null);
        System.out.println(plan.getStreamingTableRequiredColumns().requiredColumns().size());
        System.out.println(StringUtils.join(plan.getStreamingTableRequiredColumns().requiredColumns(),","));
    }


	@Test
	public void testResolveMethod(){
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{int.class}), TestClassA.class));
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{byte.class}), TestClassA.class));
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{char.class}), TestClassA.class));
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{short.class}), TestClassA.class));

		Assert.assertNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{boolean.class}), TestClassA.class));
		Assert.assertNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{String.class}), TestClassA.class));
		Assert.assertNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{long.class}), TestClassA.class));
		Assert.assertNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{float.class}), TestClassA.class));
		Assert.assertNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{double.class}), TestClassA.class));

		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{int.class, boolean.class, String.class}), TestClassB.class));
		
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{double.class, float.class, char.class}), TestClassC.class));
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{float.class, float.class, char.class}), TestClassC.class));
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{long.class, float.class, char.class}), TestClassC.class));
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{long.class, long.class, char.class}), TestClassC.class));
		Assert.assertNotNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{int.class, int.class, byte.class}), TestClassC.class));

		Assert.assertNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{long.class, double.class, char.class}), TestClassC.class));
		Assert.assertNull(ReflectionUtils.tryGetEvalMethod(asList(new Class<?>[]{long.class, double.class, int.class}), TestClassC.class));
		
	}
	
	private static List<Class<?>> asList(Class<?>[] types) {
		assert types != null;
		return Arrays.asList(types);
	}
	
	private static class TestClassA extends AbstractUDF {
		
		public void evaluate(int a)
		{
			
		}
	}
	
	private static class TestClassB extends AbstractUDF {
		
		public void evaluate(int a, boolean b, String s)
		{
			
		}
	}
	
	private static class TestClassC extends AbstractUDF {
		
		public void evaluate(double d, float f, char c)
		{
			
		}
	}
}
