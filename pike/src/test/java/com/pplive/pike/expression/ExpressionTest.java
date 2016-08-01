package com.pplive.pike.expression;

import java.util.Random;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.expression.arithmetic.AddOp;
import com.pplive.pike.expression.arithmetic.DivisionOp;
import com.pplive.pike.expression.arithmetic.MultiOp;
import com.pplive.pike.expression.arithmetic.SubtractOp;
import com.pplive.pike.expression.compare.EqualToOp;
import com.pplive.pike.expression.compare.GreaterThanOp;
import com.pplive.pike.expression.compare.GreaterThanOrEqualOp;
import com.pplive.pike.expression.compare.LessThanOp;
import com.pplive.pike.expression.compare.LessThanOrEqualOp;
import com.pplive.pike.expression.compare.NotEqualOp;
import com.pplive.pike.expression.unary.InverseOp;
import com.pplive.pike.expression.unary.IsNotNullOp;
import com.pplive.pike.expression.unary.IsNullOp;
import com.pplive.pike.expression.unary.NotOp;

public class ExpressionTest  extends BaseUnitTest {

	private Random _rand;
	
	@Before
	public void setUp() {
		_rand = new Random();
	}
	
	@Test
	public void testAdd() {
		Assert.assertEquals(AddOp.eval((Byte)null, randomByte()), null);
		Assert.assertEquals(AddOp.eval(randomByte(), (Byte)null), null);
		Assert.assertTrue(AddOp.eval((byte)0, (byte)0) == 0);
		Assert.assertTrue(AddOp.eval((byte)0, (byte)1) == 1);
		Assert.assertTrue(AddOp.eval((byte)1, (byte)0) == 1);
		Assert.assertTrue(AddOp.eval((byte)0, (byte)-1) == -1);
		Assert.assertTrue(AddOp.eval((byte)-1, (byte)0) == -1);
		Assert.assertTrue(AddOp.eval((byte)1, (byte)127) == 128);
		Assert.assertTrue(AddOp.eval((byte)-1, (byte)-128) == -129);
		Byte b1 = randomNonNullByte();
		Byte b2 = randomNonNullByte();
		Assert.assertEquals(AddOp.eval(b1, b2), AddOp.eval(b2, b1));
		Assert.assertTrue(AddOp.eval(0, b1) == b1.intValue());
		Assert.assertTrue(AddOp.eval(b1, 0) == b1.intValue());
		
		Assert.assertEquals(AddOp.eval((Byte)null, randomShort()), null);
		Assert.assertEquals(AddOp.eval((Short)null, randomShort()), null);
		Assert.assertEquals(AddOp.eval(randomShort(), (Byte)null), null);
		Assert.assertEquals(AddOp.eval(randomShort(), (Short)null), null);
		Assert.assertTrue(AddOp.eval((short)0, (short)0) == 0);
		Assert.assertTrue(AddOp.eval((short)0, (short)1) == 1);
		Assert.assertTrue(AddOp.eval((short)1, (short)0) == 1);
		Assert.assertTrue(AddOp.eval((short)0, (short)-1) == -1);
		Assert.assertTrue(AddOp.eval((short)-1, (short)0) == -1);
		Assert.assertTrue(AddOp.eval((short)1, (short)32767) == 32768);
		Assert.assertTrue(AddOp.eval((short)-1, (short)-32768) == -32769);
		Short s1 = randomNonNullShort();
		Short s2 = randomNonNullShort();
		Assert.assertEquals(AddOp.eval(s1, s2), AddOp.eval(s2, s1));
		Assert.assertEquals(AddOp.eval(s1, b2), AddOp.eval(b2, s1));
		Assert.assertTrue(AddOp.eval(0, s1) == s1.intValue());
		Assert.assertTrue(AddOp.eval(s1, 0) == s1.intValue());
		
		Assert.assertEquals(AddOp.eval((Integer)null, randomByte()), null);
		Assert.assertEquals(AddOp.eval((Integer)null, randomShort()), null);
		Assert.assertEquals(AddOp.eval((Integer)null, randomInt()), null);
		Assert.assertEquals(AddOp.eval(randomByte(), (Integer)null), null);
		Assert.assertEquals(AddOp.eval(randomShort(), (Integer)null), null);
		Assert.assertEquals(AddOp.eval(randomInt(), (Integer)null), null);
		Assert.assertTrue(AddOp.eval((int)0, (int)0) == 0);
		Assert.assertTrue(AddOp.eval((int)0, (int)1) == 1);
		Assert.assertTrue(AddOp.eval((int)1, (int)0) == 1);
		Assert.assertTrue(AddOp.eval((int)0, (int)-1) == -1);
		Assert.assertTrue(AddOp.eval((int)-1, (int)0) == -1);
		Assert.assertTrue(AddOp.eval((int)1, Integer.MAX_VALUE) == Integer.MIN_VALUE);
		Assert.assertTrue(AddOp.eval((int)-1, Integer.MIN_VALUE) == Integer.MAX_VALUE);
		Integer n1 = randomNonNullInt();
		Integer n2 = randomNonNullInt();
		Assert.assertEquals(AddOp.eval(n1, n2), AddOp.eval(n2, n1));
		Assert.assertEquals(AddOp.eval(n1, b2), AddOp.eval(b2, n1));
		Assert.assertEquals(AddOp.eval(n1, s2), AddOp.eval(s2, n1));
		Assert.assertTrue(AddOp.eval(0, n1) == n1.intValue());
		Assert.assertTrue(AddOp.eval(n1, 0) == n1.intValue());

		Assert.assertEquals(AddOp.eval((Long)null, randomByte()), null);
		Assert.assertEquals(AddOp.eval((Long)null, randomShort()), null);
		Assert.assertEquals(AddOp.eval((Long)null, randomInt()), null);
		Assert.assertEquals(AddOp.eval((Long)null, randomLong()), null);
		Assert.assertEquals(AddOp.eval(randomByte(), (Long)null), null);
		Assert.assertEquals(AddOp.eval(randomShort(), (Long)null), null);
		Assert.assertEquals(AddOp.eval(randomInt(), (Long)null), null);
		Assert.assertEquals(AddOp.eval(randomLong(), (Long)null), null);
		Assert.assertTrue(AddOp.eval((long)0, (long)0) == 0);
		Assert.assertTrue(AddOp.eval((long)0, (long)1) == 1);
		Assert.assertTrue(AddOp.eval((long)1, (long)0) == 1);
		Assert.assertTrue(AddOp.eval((long)0, (long)-1) == -1);
		Assert.assertTrue(AddOp.eval((long)-1, (long)0) == -1);
		Assert.assertTrue(AddOp.eval((long)1, Long.MAX_VALUE) == Long.MIN_VALUE);
		Assert.assertTrue(AddOp.eval((long)-1, Long.MIN_VALUE) == Long.MAX_VALUE);
		Long L1 = randomNonNullLong();
		Long L2 = randomNonNullLong();
		Assert.assertEquals(AddOp.eval(L1, L2), AddOp.eval(L2, L1));
		Assert.assertEquals(AddOp.eval(L1, b2), AddOp.eval(b2, L1));
		Assert.assertEquals(AddOp.eval(L1, s2), AddOp.eval(s2, L1));
		Assert.assertEquals(AddOp.eval(L1, n2), AddOp.eval(n2, L1));
		Assert.assertTrue(AddOp.eval(0, L1) == L1.longValue());
		Assert.assertTrue(AddOp.eval(L1, 0) == L1.longValue());
		
		Assert.assertEquals(AddOp.eval((Float)null, randomByte()), null);
		Assert.assertEquals(AddOp.eval((Float)null, randomShort()), null);
		Assert.assertEquals(AddOp.eval((Float)null, randomInt()), null);
		Assert.assertEquals(AddOp.eval((Float)null, randomLong()), null);
		Assert.assertEquals(AddOp.eval((Float)null, randomFloat()), null);
		Assert.assertEquals(AddOp.eval(randomByte(), (Float)null), null);
		Assert.assertEquals(AddOp.eval(randomShort(), (Float)null), null);
		Assert.assertEquals(AddOp.eval(randomInt(), (Float)null), null);
		Assert.assertEquals(AddOp.eval(randomLong(), (Float)null), null);
		Assert.assertEquals(AddOp.eval(randomFloat(), (Float)null), null);
		Assert.assertTrue(AddOp.eval((float)0, (float)0) == 0);
		Assert.assertTrue(AddOp.eval((float)0, (float)1) == 1);
		Assert.assertTrue(AddOp.eval((float)1, (float)0) == 1);
		Assert.assertTrue(AddOp.eval((float)0, (float)-1) == -1);
		Assert.assertTrue(AddOp.eval((float)-1, (float)0) == -1);
		Float f1 = randomNonNullFloat();
		Float f2 = randomNonNullFloat();
		Assert.assertEquals(AddOp.eval(f1, f2), AddOp.eval(f2, f1));
		Assert.assertEquals(AddOp.eval(f1, b2), AddOp.eval(b2, f1));
		Assert.assertEquals(AddOp.eval(f1, s2), AddOp.eval(s2, f1));
		Assert.assertEquals(AddOp.eval(f1, n2), AddOp.eval(n2, f1));
		Assert.assertEquals(AddOp.eval(f1, L2), AddOp.eval(L2, f1));
		Assert.assertTrue(AddOp.eval(0, f1) == f1.floatValue());
		Assert.assertTrue(AddOp.eval(f1, 0) == f1.floatValue());
		
		Assert.assertEquals(AddOp.eval((Double)null, randomByte()), null);
		Assert.assertEquals(AddOp.eval((Double)null, randomShort()), null);
		Assert.assertEquals(AddOp.eval((Double)null, randomInt()), null);
		Assert.assertEquals(AddOp.eval((Double)null, randomLong()), null);
		Assert.assertEquals(AddOp.eval((Double)null, randomFloat()), null);
		Assert.assertEquals(AddOp.eval((Double)null, randomDouble()), null);
		Assert.assertEquals(AddOp.eval(randomByte(), (Double)null), null);
		Assert.assertEquals(AddOp.eval(randomShort(), (Double)null), null);
		Assert.assertEquals(AddOp.eval(randomInt(), (Double)null), null);
		Assert.assertEquals(AddOp.eval(randomLong(), (Double)null), null);
		Assert.assertEquals(AddOp.eval(randomFloat(), (Double)null), null);
		Assert.assertEquals(AddOp.eval(randomDouble(), (Double)null), null);
		Assert.assertTrue(AddOp.eval((double)0, (double)0) == 0);
		Assert.assertTrue(AddOp.eval((double)0, (double)1) == 1);
		Assert.assertTrue(AddOp.eval((double)1, (double)0) == 1);
		Assert.assertTrue(AddOp.eval((double)0, (double)-1) == -1);
		Assert.assertTrue(AddOp.eval((double)-1, (double)0) == -1);
		Double d1 = randomNonNullDouble();
		Double d2 = randomNonNullDouble();
		Assert.assertEquals(AddOp.eval(d1, d2), AddOp.eval(d2, d1));
		Assert.assertEquals(AddOp.eval(d1, b2), AddOp.eval(b2, d1));
		Assert.assertEquals(AddOp.eval(d1, s2), AddOp.eval(s2, d1));
		Assert.assertEquals(AddOp.eval(d1, n2), AddOp.eval(n2, d1));
		Assert.assertEquals(AddOp.eval(d1, L2), AddOp.eval(L2, d1));
		Assert.assertEquals(AddOp.eval(d1, f2), AddOp.eval(f2, d1));
		Assert.assertTrue(AddOp.eval(0, d1) == d1.doubleValue());
		Assert.assertTrue(AddOp.eval(d1, 0) == d1.doubleValue());
		
		Assert.assertEquals(AddOp.eval(null, randomString()), null);
		Assert.assertEquals(AddOp.eval(randomString(), null), null);
		Assert.assertEquals(AddOp.eval("", ""), "");
		String str1 = randomNonNullString();
		String str2 = randomNonNullString();
		Assert.assertEquals(AddOp.eval(str1, str2), str1 + str2);
		Assert.assertEquals(AddOp.eval(str1, ""), str1);
		Assert.assertEquals(AddOp.eval("", str1), str1);
		
	}
	
	@Test
	public void testSub() {
		Assert.assertEquals(SubtractOp.eval((Byte)null, randomByte()), null);
		Assert.assertEquals(SubtractOp.eval(randomByte(), (Byte)null), null);
		Assert.assertTrue(SubtractOp.eval((byte)0, (byte)0) == 0);
		Assert.assertTrue(SubtractOp.eval((byte)0, (byte)1) == -1);
		Assert.assertTrue(SubtractOp.eval((byte)1, (byte)0) == 1);
		Assert.assertTrue(SubtractOp.eval((byte)0, (byte)-1) == 1);
		Assert.assertTrue(SubtractOp.eval((byte)-1, (byte)0) == -1);
		Assert.assertTrue(SubtractOp.eval((byte)0, (byte)-128) == 128);
		Assert.assertTrue(SubtractOp.eval((byte)-128, (byte)1) == -129);
		Byte b1 = randomNonNullByte();
		Byte b2 = randomNonNullByte();
		Assert.assertTrue(SubtractOp.eval(b1, b2) == -SubtractOp.eval(b2, b1));
		Assert.assertTrue(SubtractOp.eval(0, b1) == -b1.intValue());
		Assert.assertTrue(SubtractOp.eval(b1, 0) == b1.intValue());
		
		Assert.assertEquals(SubtractOp.eval((Byte)null, randomShort()), null);
		Assert.assertEquals(SubtractOp.eval((Short)null, randomShort()), null);
		Assert.assertEquals(SubtractOp.eval(randomShort(), (Byte)null), null);
		Assert.assertEquals(SubtractOp.eval(randomShort(), (Short)null), null);
		Assert.assertTrue(SubtractOp.eval((short)0, (short)0) == 0);
		Assert.assertTrue(SubtractOp.eval((short)0, (short)1) == -1);
		Assert.assertTrue(SubtractOp.eval((short)1, (short)0) == 1);
		Assert.assertTrue(SubtractOp.eval((short)0, (short)-1) == 1);
		Assert.assertTrue(SubtractOp.eval((short)-1, (short)0) == -1);
		Assert.assertTrue(SubtractOp.eval((short)0, (short)-32768) == 32768);
		Assert.assertTrue(SubtractOp.eval((short)-32768, (short)1) == -32769);
		Short s1 = randomNonNullShort();
		Short s2 = randomNonNullShort();
		Assert.assertTrue(SubtractOp.eval(s1, s2) == -SubtractOp.eval(s2, s1));
		Assert.assertTrue(SubtractOp.eval(s1, b2) == -SubtractOp.eval(b2, s1));
		Assert.assertTrue(SubtractOp.eval(0, s1) == -s1.intValue());
		Assert.assertTrue(SubtractOp.eval(s1, 0) == s1.intValue());
		
		Assert.assertEquals(SubtractOp.eval((Integer)null, randomByte()), null);
		Assert.assertEquals(SubtractOp.eval((Integer)null, randomShort()), null);
		Assert.assertEquals(SubtractOp.eval((Integer)null, randomInt()), null);
		Assert.assertEquals(SubtractOp.eval(randomByte(), (Integer)null), null);
		Assert.assertEquals(SubtractOp.eval(randomShort(), (Integer)null), null);
		Assert.assertEquals(SubtractOp.eval(randomInt(), (Integer)null), null);
		Assert.assertTrue(SubtractOp.eval((int)0, (int)0) == 0);
		Assert.assertTrue(SubtractOp.eval((int)0, (int)1) == -1);
		Assert.assertTrue(SubtractOp.eval((int)1, (int)0) == 1);
		Assert.assertTrue(SubtractOp.eval((int)0, (int)-1) == 1);
		Assert.assertTrue(SubtractOp.eval((int)-1, (int)0) == -1);
		Assert.assertTrue(SubtractOp.eval((int)0, Integer.MIN_VALUE) == Integer.MIN_VALUE);
		Assert.assertTrue(SubtractOp.eval(Integer.MIN_VALUE, (int)1) == Integer.MAX_VALUE);
		Integer n1 = randomNonNullInt();
		Integer n2 = randomNonNullInt();
		Assert.assertTrue(SubtractOp.eval(n1, n2) == -SubtractOp.eval(n2, n1));
		Assert.assertTrue(SubtractOp.eval(n1, b2) == -SubtractOp.eval(b2, n1));
		Assert.assertTrue(SubtractOp.eval(n1, s2) == -SubtractOp.eval(s2, n1));
		Assert.assertTrue(SubtractOp.eval(0, n1) == -n1.intValue());
		Assert.assertTrue(SubtractOp.eval(n1, 0) == n1.intValue());

		Assert.assertEquals(SubtractOp.eval((Long)null, randomByte()), null);
		Assert.assertEquals(SubtractOp.eval((Long)null, randomShort()), null);
		Assert.assertEquals(SubtractOp.eval((Long)null, randomInt()), null);
		Assert.assertEquals(SubtractOp.eval((Long)null, randomLong()), null);
		Assert.assertEquals(SubtractOp.eval(randomByte(), (Long)null), null);
		Assert.assertEquals(SubtractOp.eval(randomShort(), (Long)null), null);
		Assert.assertEquals(SubtractOp.eval(randomInt(), (Long)null), null);
		Assert.assertEquals(SubtractOp.eval(randomLong(), (Long)null), null);
		Assert.assertTrue(SubtractOp.eval((long)0, (long)0) == 0);
		Assert.assertTrue(SubtractOp.eval((long)0, (long)1) == -1);
		Assert.assertTrue(SubtractOp.eval((long)1, (long)0) == 1);
		Assert.assertTrue(SubtractOp.eval((long)0, (long)-1) == 1);
		Assert.assertTrue(SubtractOp.eval((long)-1, (long)0) == -1);
		Assert.assertTrue(SubtractOp.eval((long)0, Long.MIN_VALUE) == Long.MIN_VALUE);
		Assert.assertTrue(SubtractOp.eval(Long.MIN_VALUE, (long)1) == Long.MAX_VALUE);
		Long L1 = randomNonNullLong();
		Long L2 = randomNonNullLong();
		Assert.assertTrue(SubtractOp.eval(L1, L2) == -SubtractOp.eval(L2, L1));
		Assert.assertTrue(SubtractOp.eval(L1, b2) == -SubtractOp.eval(b2, L1));
		Assert.assertTrue(SubtractOp.eval(L1, s2) == -SubtractOp.eval(s2, L1));
		Assert.assertTrue(SubtractOp.eval(L1, n2) == -SubtractOp.eval(n2, L1));
		Assert.assertTrue(SubtractOp.eval(0, L1) == -L1.longValue());
		Assert.assertTrue(SubtractOp.eval(L1, 0) == L1.longValue());
		
		Assert.assertEquals(SubtractOp.eval((Float)null, randomByte()), null);
		Assert.assertEquals(SubtractOp.eval((Float)null, randomShort()), null);
		Assert.assertEquals(SubtractOp.eval((Float)null, randomInt()), null);
		Assert.assertEquals(SubtractOp.eval((Float)null, randomLong()), null);
		Assert.assertEquals(SubtractOp.eval((Float)null, randomFloat()), null);
		Assert.assertEquals(SubtractOp.eval(randomByte(), (Float)null), null);
		Assert.assertEquals(SubtractOp.eval(randomShort(), (Float)null), null);
		Assert.assertEquals(SubtractOp.eval(randomInt(), (Float)null), null);
		Assert.assertEquals(SubtractOp.eval(randomLong(), (Float)null), null);
		Assert.assertEquals(SubtractOp.eval(randomFloat(), (Float)null), null);
		Assert.assertTrue(SubtractOp.eval((float)0, (float)0) == 0);
		Assert.assertTrue(SubtractOp.eval((float)0, (float)1) == -1);
		Assert.assertTrue(SubtractOp.eval((float)1, (float)0) == 1);
		Assert.assertTrue(SubtractOp.eval((float)0, (float)-1) == 1);
		Assert.assertTrue(SubtractOp.eval((float)-1, (float)0) == -1);
		Float f1 = randomNonNullFloat();
		Float f2 = randomNonNullFloat();
		Assert.assertTrue(SubtractOp.eval(f1, f2) == -SubtractOp.eval(f2, f1));
		Assert.assertTrue(SubtractOp.eval(f1, b2) == -SubtractOp.eval(b2, f1));
		Assert.assertTrue(SubtractOp.eval(f1, s2) == -SubtractOp.eval(s2, f1));
		Assert.assertTrue(SubtractOp.eval(f1, n2) == -SubtractOp.eval(n2, f1));
		Assert.assertTrue(SubtractOp.eval(f1, L2) == -SubtractOp.eval(L2, f1));
		Assert.assertTrue(SubtractOp.eval(0, f1) == -f1.floatValue());
		Assert.assertTrue(SubtractOp.eval(f1, 0) == f1.floatValue());
		
		Assert.assertEquals(SubtractOp.eval((Double)null, randomByte()), null);
		Assert.assertEquals(SubtractOp.eval((Double)null, randomShort()), null);
		Assert.assertEquals(SubtractOp.eval((Double)null, randomInt()), null);
		Assert.assertEquals(SubtractOp.eval((Double)null, randomLong()), null);
		Assert.assertEquals(SubtractOp.eval((Double)null, randomFloat()), null);
		Assert.assertEquals(SubtractOp.eval((Double)null, randomDouble()), null);
		Assert.assertEquals(SubtractOp.eval(randomByte(), (Double)null), null);
		Assert.assertEquals(SubtractOp.eval(randomShort(), (Double)null), null);
		Assert.assertEquals(SubtractOp.eval(randomInt(), (Double)null), null);
		Assert.assertEquals(SubtractOp.eval(randomLong(), (Double)null), null);
		Assert.assertEquals(SubtractOp.eval(randomFloat(), (Double)null), null);
		Assert.assertEquals(SubtractOp.eval(randomDouble(), (Double)null), null);
		Assert.assertTrue(SubtractOp.eval((double)0, (double)0) == 0);
		Assert.assertTrue(SubtractOp.eval((double)0, (double)1) == -1);
		Assert.assertTrue(SubtractOp.eval((double)1, (double)0) == 1);
		Assert.assertTrue(SubtractOp.eval((double)0, (double)-1) == 1);
		Assert.assertTrue(SubtractOp.eval((double)-1, (double)0) == -1);
		Double d1 = randomNonNullDouble();
		Double d2 = randomNonNullDouble();
		Assert.assertTrue(SubtractOp.eval(d1, d2) == -SubtractOp.eval(d2, d1));
		Assert.assertTrue(SubtractOp.eval(d1, b2) == -SubtractOp.eval(b2, d1));
		Assert.assertTrue(SubtractOp.eval(d1, s2) == -SubtractOp.eval(s2, d1));
		Assert.assertTrue(SubtractOp.eval(d1, n2) == -SubtractOp.eval(n2, d1));
		Assert.assertTrue(SubtractOp.eval(d1, L2) == -SubtractOp.eval(L2, d1));
		Assert.assertTrue(SubtractOp.eval(d1, f2) == -SubtractOp.eval(f2, d1));
		Assert.assertTrue(SubtractOp.eval(0, d1) == -d1.doubleValue());
		Assert.assertTrue(SubtractOp.eval(d1, 0) == d1.doubleValue());
		
	}
	
	@Test
	public void testMultiply() {
		Assert.assertEquals(MultiOp.eval((Byte)null, randomByte()), null);
		Assert.assertEquals(MultiOp.eval(randomByte(), (Byte)null), null);
		Byte b1 = randomNonNullByte();
		Byte b2 = randomNonNullByte();
		Assert.assertTrue(MultiOp.eval(b1, b2) == MultiOp.eval(b2, b1).intValue());
		Assert.assertTrue(MultiOp.eval(0, b1) == 0);
		Assert.assertTrue(MultiOp.eval(b1, 0) == 0);
		Assert.assertTrue(MultiOp.eval(1, b1) == b1.intValue());
		Assert.assertTrue(MultiOp.eval(b1, 1) == b1.intValue());
		Assert.assertTrue(MultiOp.eval(-1, b1) == -b1.intValue());
		Assert.assertTrue(MultiOp.eval(b1, -1) == -b1.intValue());
		
		Assert.assertEquals(MultiOp.eval((Byte)null, randomShort()), null);
		Assert.assertEquals(MultiOp.eval((Short)null, randomShort()), null);
		Assert.assertEquals(MultiOp.eval(randomShort(), (Byte)null), null);
		Assert.assertEquals(MultiOp.eval(randomShort(), (Short)null), null);
		Short s1 = randomNonNullShort();
		Short s2 = randomNonNullShort();
		Assert.assertTrue(MultiOp.eval(s1, s2) == MultiOp.eval(s2, s1).intValue());
		Assert.assertTrue(MultiOp.eval(s1, b2) == MultiOp.eval(b2, s1).intValue());
		Assert.assertTrue(MultiOp.eval(0, s1) == 0);
		Assert.assertTrue(MultiOp.eval(s1, 0) == 0);
		Assert.assertTrue(MultiOp.eval(1, s1) == s1.intValue());
		Assert.assertTrue(MultiOp.eval(s1, 1) == s1.intValue());
		Assert.assertTrue(MultiOp.eval(-1, s1) == -s1.intValue());
		Assert.assertTrue(MultiOp.eval(s1, -1) == -s1.intValue());
		
		Assert.assertEquals(MultiOp.eval((Integer)null, randomByte()), null);
		Assert.assertEquals(MultiOp.eval((Integer)null, randomShort()), null);
		Assert.assertEquals(MultiOp.eval((Integer)null, randomInt()), null);
		Assert.assertEquals(MultiOp.eval(randomByte(), (Integer)null), null);
		Assert.assertEquals(MultiOp.eval(randomShort(), (Integer)null), null);
		Assert.assertEquals(MultiOp.eval(randomInt(), (Integer)null), null);
		Integer n1 = randomNonNullInt();
		Integer n2 = randomNonNullInt();
		Assert.assertTrue(MultiOp.eval(n1, n2) == MultiOp.eval(n2, n1).intValue());
		Assert.assertTrue(MultiOp.eval(n1, b2) == MultiOp.eval(b2, n1).intValue());
		Assert.assertTrue(MultiOp.eval(n1, s2) == MultiOp.eval(s2, n1).intValue());
		Assert.assertTrue(MultiOp.eval(0, n1) == 0);
		Assert.assertTrue(MultiOp.eval(n1, 0) == 0);
		Assert.assertTrue(MultiOp.eval(1, n1) == n1.intValue());
		Assert.assertTrue(MultiOp.eval(n1, 1) == n1.intValue());
		Assert.assertTrue(MultiOp.eval(-1, n1) == -n1.intValue());
		Assert.assertTrue(MultiOp.eval(n1, -1) == -n1.intValue());

		Assert.assertEquals(MultiOp.eval((Long)null, randomByte()), null);
		Assert.assertEquals(MultiOp.eval((Long)null, randomShort()), null);
		Assert.assertEquals(MultiOp.eval((Long)null, randomInt()), null);
		Assert.assertEquals(MultiOp.eval((Long)null, randomLong()), null);
		Assert.assertEquals(MultiOp.eval(randomByte(), (Long)null), null);
		Assert.assertEquals(MultiOp.eval(randomShort(), (Long)null), null);
		Assert.assertEquals(MultiOp.eval(randomInt(), (Long)null), null);
		Assert.assertEquals(MultiOp.eval(randomLong(), (Long)null), null);
		Long L1 = randomNonNullLong();
		Long L2 = randomNonNullLong();
		Assert.assertTrue(MultiOp.eval(L1, L2) == MultiOp.eval(L2, L1).longValue());
		Assert.assertTrue(MultiOp.eval(L1, b2) == MultiOp.eval(b2, L1).longValue());
		Assert.assertTrue(MultiOp.eval(L1, s2) == MultiOp.eval(s2, L1).longValue());
		Assert.assertTrue(MultiOp.eval(L1, n2) == MultiOp.eval(n2, L1).longValue());
		Assert.assertTrue(MultiOp.eval(0, L1) == 0);
		Assert.assertTrue(MultiOp.eval(L1, 0) == 0);
		Assert.assertTrue(MultiOp.eval(1, L1) == L1.longValue());
		Assert.assertTrue(MultiOp.eval(L1, 1) == L1.longValue());
		Assert.assertTrue(MultiOp.eval(-1, L1) == -L1.longValue());
		Assert.assertTrue(MultiOp.eval(L1, -1) == -L1.longValue());
		
		Assert.assertEquals(MultiOp.eval((Float)null, randomByte()), null);
		Assert.assertEquals(MultiOp.eval((Float)null, randomShort()), null);
		Assert.assertEquals(MultiOp.eval((Float)null, randomInt()), null);
		Assert.assertEquals(MultiOp.eval((Float)null, randomLong()), null);
		Assert.assertEquals(MultiOp.eval((Float)null, randomFloat()), null);
		Assert.assertEquals(MultiOp.eval(randomByte(), (Float)null), null);
		Assert.assertEquals(MultiOp.eval(randomShort(), (Float)null), null);
		Assert.assertEquals(MultiOp.eval(randomInt(), (Float)null), null);
		Assert.assertEquals(MultiOp.eval(randomLong(), (Float)null), null);
		Assert.assertEquals(MultiOp.eval(randomFloat(), (Float)null), null);
		Float f1 = randomNonNullFloat();
		Float f2 = randomNonNullFloat();
		Assert.assertTrue(MultiOp.eval(f1, f2) == MultiOp.eval(f2, f1).floatValue());
		Assert.assertTrue(MultiOp.eval(f1, b2) == MultiOp.eval(b2, f1).floatValue());
		Assert.assertTrue(MultiOp.eval(f1, s2) == MultiOp.eval(s2, f1).floatValue());
		Assert.assertTrue(MultiOp.eval(f1, n2) == MultiOp.eval(n2, f1).floatValue());
		Assert.assertTrue(MultiOp.eval(f1, L2) == MultiOp.eval(L2, f1).floatValue());
		Assert.assertTrue(MultiOp.eval(0, f1) == 0);
		Assert.assertTrue(MultiOp.eval(f1, 0) == 0);
		Assert.assertTrue(MultiOp.eval(1, f1) == f1.floatValue());
		Assert.assertTrue(MultiOp.eval(f1, 1) == f1.floatValue());
		Assert.assertTrue(MultiOp.eval(-1, f1) == -f1.floatValue());
		Assert.assertTrue(MultiOp.eval(f1, -1) == -f1.floatValue());
		
		Assert.assertEquals(MultiOp.eval((Double)null, randomByte()), null);
		Assert.assertEquals(MultiOp.eval((Double)null, randomShort()), null);
		Assert.assertEquals(MultiOp.eval((Double)null, randomInt()), null);
		Assert.assertEquals(MultiOp.eval((Double)null, randomLong()), null);
		Assert.assertEquals(MultiOp.eval((Double)null, randomFloat()), null);
		Assert.assertEquals(MultiOp.eval((Double)null, randomDouble()), null);
		Assert.assertEquals(MultiOp.eval(randomByte(), (Double)null), null);
		Assert.assertEquals(MultiOp.eval(randomShort(), (Double)null), null);
		Assert.assertEquals(MultiOp.eval(randomInt(), (Double)null), null);
		Assert.assertEquals(MultiOp.eval(randomLong(), (Double)null), null);
		Assert.assertEquals(MultiOp.eval(randomFloat(), (Double)null), null);
		Assert.assertEquals(MultiOp.eval(randomDouble(), (Double)null), null);
		Double d1 = randomNonNullDouble();
		Double d2 = randomNonNullDouble();
		Assert.assertTrue(MultiOp.eval(d1, d2) == MultiOp.eval(d2, d1).doubleValue());
		Assert.assertTrue(MultiOp.eval(d1, b2) == MultiOp.eval(b2, d1).doubleValue());
		Assert.assertTrue(MultiOp.eval(d1, s2) == MultiOp.eval(s2, d1).doubleValue());
		Assert.assertTrue(MultiOp.eval(d1, n2) == MultiOp.eval(n2, d1).doubleValue());
		Assert.assertTrue(MultiOp.eval(d1, L2) == MultiOp.eval(L2, d1).doubleValue());
		Assert.assertTrue(MultiOp.eval(d1, f2) == MultiOp.eval(f2, d1).doubleValue());
		Assert.assertTrue(MultiOp.eval(0, d1) == 0);
		Assert.assertTrue(MultiOp.eval(d1, 0) == 0);
		Assert.assertTrue(MultiOp.eval(1, d1) == d1.doubleValue());
		Assert.assertTrue(MultiOp.eval(d1, 1) == d1.doubleValue());
		Assert.assertTrue(MultiOp.eval(-1, d1) == -d1.doubleValue());
		Assert.assertTrue(MultiOp.eval(d1, -1) == -d1.doubleValue());
		
	}
	
	@Test
	public void testDivide() {
		Assert.assertEquals(DivisionOp.eval((Byte)null, randomByte()), null);
		Assert.assertEquals(DivisionOp.eval(randomByte(), (Byte)null), null);
		Byte b1 = randomNonZeroByte();
		Byte b2 = randomNonZeroByte();
		Assert.assertTrue(DivisionOp.eval(b1, b2) == b1 / b2);
		Assert.assertTrue(DivisionOp.eval(0, b1) == 0);
		Assert.assertTrue(DivisionOp.eval(1, b1) == 1 / b1.intValue());
		Assert.assertTrue(DivisionOp.eval(b1, 1) == b1.intValue());
		Assert.assertTrue(DivisionOp.eval(-1, b1) == -1 / b1.intValue());
		Assert.assertTrue(DivisionOp.eval(b1, -1) == -b1.intValue());
		
		Assert.assertEquals(DivisionOp.eval((Byte)null, randomShort()), null);
		Assert.assertEquals(DivisionOp.eval((Short)null, randomShort()), null);
		Assert.assertEquals(DivisionOp.eval(randomShort(), (Byte)null), null);
		Assert.assertEquals(DivisionOp.eval(randomShort(), (Short)null), null);
		Short s1 = randomNonZeroShort();
		Short s2 = randomNonZeroShort();
		Assert.assertTrue(DivisionOp.eval(s1, s2) == s1 / s2);
		Assert.assertTrue(DivisionOp.eval(s1, b2) == s1 / b2);
		Assert.assertTrue(DivisionOp.eval(0, s1) == 0);
		Assert.assertTrue(DivisionOp.eval(1, s1) == 1 / s1.intValue());
		Assert.assertTrue(DivisionOp.eval(s1, 1) == s1.intValue());
		Assert.assertTrue(DivisionOp.eval(-1, s1) == -1 / s1.intValue());
		Assert.assertTrue(DivisionOp.eval(s1, -1) == -s1.intValue());
		
		Assert.assertEquals(DivisionOp.eval((Integer)null, randomByte()), null);
		Assert.assertEquals(DivisionOp.eval((Integer)null, randomShort()), null);
		Assert.assertEquals(DivisionOp.eval((Integer)null, randomInt()), null);
		Assert.assertEquals(DivisionOp.eval(randomByte(), (Integer)null), null);
		Assert.assertEquals(DivisionOp.eval(randomShort(), (Integer)null), null);
		Assert.assertEquals(DivisionOp.eval(randomInt(), (Integer)null), null);
		Integer n1 = randomNonZeroInt();
		Integer n2 = randomNonZeroInt();
		Assert.assertTrue(DivisionOp.eval(n1, n2) == n1 / n2);
		Assert.assertTrue(DivisionOp.eval(n1, b2) == n1 / b2);
		Assert.assertTrue(DivisionOp.eval(n1, s2) == n1 / s2);
		Assert.assertTrue(DivisionOp.eval(0, n1) == 0);
		Assert.assertTrue(DivisionOp.eval(1, n1) == 1 / n1.intValue());
		Assert.assertTrue(DivisionOp.eval(n1, 1) == n1.intValue());
		Assert.assertTrue(DivisionOp.eval(-1, n1) == -1 / n1.intValue());
		Assert.assertTrue(DivisionOp.eval(n1, -1) == -n1.intValue());

		Assert.assertEquals(DivisionOp.eval((Long)null, randomByte()), null);
		Assert.assertEquals(DivisionOp.eval((Long)null, randomShort()), null);
		Assert.assertEquals(DivisionOp.eval((Long)null, randomInt()), null);
		Assert.assertEquals(DivisionOp.eval((Long)null, randomLong()), null);
		Assert.assertEquals(DivisionOp.eval(randomByte(), (Long)null), null);
		Assert.assertEquals(DivisionOp.eval(randomShort(), (Long)null), null);
		Assert.assertEquals(DivisionOp.eval(randomInt(), (Long)null), null);
		Assert.assertEquals(DivisionOp.eval(randomLong(), (Long)null), null);
		Long L1 = randomNonZeroLong();
		Long L2 = randomNonZeroLong();
		Assert.assertTrue(DivisionOp.eval(L1, L2) == L1 / L2);
		Assert.assertTrue(DivisionOp.eval(L1, b2) == L1 / b2);
		Assert.assertTrue(DivisionOp.eval(L1, s2) == L1 / s2);
		Assert.assertTrue(DivisionOp.eval(L1, n2) == L1 / n2);
		Assert.assertTrue(DivisionOp.eval(0, L1) == 0);
		Assert.assertTrue(DivisionOp.eval(1, L1) == 1 / L1.longValue());
		Assert.assertTrue(DivisionOp.eval(L1, 1) == L1.longValue());
		Assert.assertTrue(DivisionOp.eval(-1, L1) == -1 / L1.longValue());
		Assert.assertTrue(DivisionOp.eval(L1, -1) == -L1.longValue());
		
		Assert.assertEquals(DivisionOp.eval((Float)null, randomByte()), null);
		Assert.assertEquals(DivisionOp.eval((Float)null, randomShort()), null);
		Assert.assertEquals(DivisionOp.eval((Float)null, randomInt()), null);
		Assert.assertEquals(DivisionOp.eval((Float)null, randomLong()), null);
		Assert.assertEquals(DivisionOp.eval((Float)null, randomFloat()), null);
		Assert.assertEquals(DivisionOp.eval(randomByte(), (Float)null), null);
		Assert.assertEquals(DivisionOp.eval(randomShort(), (Float)null), null);
		Assert.assertEquals(DivisionOp.eval(randomInt(), (Float)null), null);
		Assert.assertEquals(DivisionOp.eval(randomLong(), (Float)null), null);
		Assert.assertEquals(DivisionOp.eval(randomFloat(), (Float)null), null);
		Float f1 = randomNonZeroFloat();
		Float f2 = randomNonZeroFloat();
		Assert.assertTrue(DivisionOp.eval(f1, f2) == f1 / f2);
		Assert.assertTrue(DivisionOp.eval(f1, b2) == f1 / b2);
		Assert.assertTrue(DivisionOp.eval(f1, s2) == f1 / s2);
		Assert.assertTrue(DivisionOp.eval(f1, n2) == f1 / n2);
		Assert.assertTrue(DivisionOp.eval(f1, L2) == f1 / L2);
		Assert.assertTrue(DivisionOp.eval(0, f1) == 0);
		Assert.assertTrue(DivisionOp.eval(1, f1) == 1 / f1.floatValue());
		Assert.assertTrue(DivisionOp.eval(f1, 1) == f1.floatValue());
		Assert.assertTrue(DivisionOp.eval(-1, f1) == -1 / f1.floatValue());
		Assert.assertTrue(DivisionOp.eval(f1, -1) == -f1.floatValue());
		
		Assert.assertEquals(DivisionOp.eval((Double)null, randomByte()), null);
		Assert.assertEquals(DivisionOp.eval((Double)null, randomShort()), null);
		Assert.assertEquals(DivisionOp.eval((Double)null, randomInt()), null);
		Assert.assertEquals(DivisionOp.eval((Double)null, randomLong()), null);
		Assert.assertEquals(DivisionOp.eval((Double)null, randomFloat()), null);
		Assert.assertEquals(DivisionOp.eval((Double)null, randomDouble()), null);
		Assert.assertEquals(DivisionOp.eval(randomByte(), (Double)null), null);
		Assert.assertEquals(DivisionOp.eval(randomShort(), (Double)null), null);
		Assert.assertEquals(DivisionOp.eval(randomInt(), (Double)null), null);
		Assert.assertEquals(DivisionOp.eval(randomLong(), (Double)null), null);
		Assert.assertEquals(DivisionOp.eval(randomFloat(), (Double)null), null);
		Assert.assertEquals(DivisionOp.eval(randomDouble(), (Double)null), null);
		Double d1 = randomNonZeroDouble();
		Double d2 = randomNonZeroDouble();
		Assert.assertTrue(DivisionOp.eval(d1, d2) == d1 / d2);
		Assert.assertTrue(DivisionOp.eval(d1, b2) == d1 / b2);
		Assert.assertTrue(DivisionOp.eval(d1, s2) == d1 / s2);
		Assert.assertTrue(DivisionOp.eval(d1, n2) == d1 / n2);
		Assert.assertTrue(DivisionOp.eval(d1, L2) == d1 / L2);
		Assert.assertTrue(DivisionOp.eval(d1, f2) == d1 / f2);
		Assert.assertTrue(DivisionOp.eval(0, d1) == 0);
		Assert.assertTrue(DivisionOp.eval(1, d1) == 1 / d1.doubleValue());
		Assert.assertTrue(DivisionOp.eval(d1, 1) == d1.doubleValue());
		Assert.assertTrue(DivisionOp.eval(-1, d1) == -1 / d1.doubleValue());
		Assert.assertTrue(DivisionOp.eval(d1, -1) == -d1.doubleValue());
		
	}
	
	@Test
	public void testInverse() {
		Assert.assertEquals(InverseOp.eval((Byte)null), null);
        Byte b1 = randomNonNullByte();
        Assert.assertTrue(InverseOp.eval(b1) == (byte)-b1.byteValue());

        Assert.assertEquals(InverseOp.eval((Short)null), null);
		Short s1 = randomNonNullShort();
		Assert.assertTrue(InverseOp.eval(s1) == (short)-s1.shortValue());
		
		Assert.assertEquals(InverseOp.eval((Integer)null), null);
		Integer n1 = randomNonNullInt();
		Assert.assertTrue(InverseOp.eval(n1) == (int)-n1.intValue());

		Assert.assertEquals(InverseOp.eval((Long)null), null);
		Long L1 = randomNonNullLong();
		Assert.assertTrue(InverseOp.eval(L1) == -L1.longValue());
		
		Assert.assertEquals(InverseOp.eval((Float)null), null);
		Float f1 = randomNonNullFloat();
		Assert.assertTrue(InverseOp.eval(f1) == -f1.floatValue());
		
		Assert.assertEquals(InverseOp.eval((Double)null), null);
		Double d1 = randomNonZeroDouble();
		Assert.assertTrue(InverseOp.eval(d1) == -d1.doubleValue());
		
	}
	
	@Test
	public void testIsNull() {
		Assert.assertTrue(IsNullOp.eval(null));
		Assert.assertFalse(IsNotNullOp.eval(null));
		Assert.assertFalse(IsNullOp.eval(new Object()));
		Assert.assertTrue(IsNotNullOp.eval(new Object()));
	}
	
	@Test
	public void testNot() {
		Assert.assertNull(NotOp.eval((Boolean)null));
		Assert.assertNull(NotOp.eval((Object)null));
		
		Assert.assertTrue(NotOp.eval(false));
		Assert.assertTrue(NotOp.eval(Boolean.FALSE));
		Assert.assertFalse(NotOp.eval(true));
		Assert.assertFalse(NotOp.eval(Boolean.TRUE));
		
		Assert.assertTrue(NotOp.eval(0));
		Assert.assertTrue(NotOp.eval(0.0));
		Assert.assertFalse(NotOp.eval(1));
		Assert.assertTrue(NotOp.eval(""));
		Assert.assertTrue(NotOp.eval("false"));
		Assert.assertFalse(NotOp.eval("1"));
		Assert.assertFalse(NotOp.eval("true"));
		Assert.assertFalse(NotOp.eval("abc"));
	}
	
	@Test
	public void testEqualTo() {
		Assert.assertNull(EqualToOp.eval((Boolean)null, true));
		Assert.assertNull(EqualToOp.eval((Boolean)null, false));
		Assert.assertNull(EqualToOp.eval(true, (Boolean)null));
		Assert.assertNull(EqualToOp.eval(false, (Boolean)null));
		
		Assert.assertNull(EqualToOp.eval((Byte)null, randomByte()));
		Assert.assertNull(EqualToOp.eval(randomByte(), (Byte)null));
		Assert.assertNull(EqualToOp.eval((Short)null, randomByte()));
		Assert.assertNull(EqualToOp.eval((Short)null, randomShort()));
		Assert.assertNull(EqualToOp.eval(randomByte(), (Short)null));
		Assert.assertNull(EqualToOp.eval(randomShort(), (Short)null));
		Assert.assertNull(EqualToOp.eval((Integer)null, randomByte()));
		Assert.assertNull(EqualToOp.eval((Integer)null, randomShort()));
		Assert.assertNull(EqualToOp.eval((Integer)null, randomInt()));
		Assert.assertNull(EqualToOp.eval(randomByte(), (Integer)null));
		Assert.assertNull(EqualToOp.eval(randomShort(), (Integer)null));
		Assert.assertNull(EqualToOp.eval(randomInt(), (Integer)null));
		Assert.assertNull(EqualToOp.eval((Long)null, randomByte()));
		Assert.assertNull(EqualToOp.eval((Long)null, randomShort()));
		Assert.assertNull(EqualToOp.eval((Long)null, randomInt()));
		Assert.assertNull(EqualToOp.eval((Long)null, randomLong()));
		Assert.assertNull(EqualToOp.eval(randomByte(), (Long)null));
		Assert.assertNull(EqualToOp.eval(randomShort(), (Long)null));
		Assert.assertNull(EqualToOp.eval(randomInt(), (Long)null));
		Assert.assertNull(EqualToOp.eval(randomLong(), (Long)null));
		Assert.assertNull(EqualToOp.eval((Float)null, randomByte()));
		Assert.assertNull(EqualToOp.eval((Float)null, randomShort()));
		Assert.assertNull(EqualToOp.eval((Float)null, randomInt()));
		Assert.assertNull(EqualToOp.eval((Float)null, randomLong()));
		Assert.assertNull(EqualToOp.eval((Float)null, randomFloat()));
		Assert.assertNull(EqualToOp.eval(randomByte(), (Float)null));
		Assert.assertNull(EqualToOp.eval(randomShort(), (Float)null));
		Assert.assertNull(EqualToOp.eval(randomInt(), (Float)null));
		Assert.assertNull(EqualToOp.eval(randomLong(), (Float)null));
		Assert.assertNull(EqualToOp.eval(randomFloat(), (Float)null));
		Assert.assertNull(EqualToOp.eval((Double)null, randomByte()));
		Assert.assertNull(EqualToOp.eval((Double)null, randomShort()));
		Assert.assertNull(EqualToOp.eval((Double)null, randomInt()));
		Assert.assertNull(EqualToOp.eval((Double)null, randomLong()));
		Assert.assertNull(EqualToOp.eval((Double)null, randomFloat()));
		Assert.assertNull(EqualToOp.eval((Double)null, randomDouble()));
		Assert.assertNull(EqualToOp.eval(randomByte(), (Double)null));
		Assert.assertNull(EqualToOp.eval(randomShort(), (Double)null));
		Assert.assertNull(EqualToOp.eval(randomInt(), (Double)null));
		Assert.assertNull(EqualToOp.eval(randomLong(), (Double)null));
		Assert.assertNull(EqualToOp.eval(randomFloat(), (Double)null));
		Assert.assertNull(EqualToOp.eval(randomDouble(), (Double)null));
		Assert.assertNull(EqualToOp.eval((String)null, randomString()));
		Assert.assertNull(EqualToOp.eval(randomString(), (String)null));
		
		Assert.assertTrue(EqualToOp.eval(true, Boolean.TRUE));
		Assert.assertTrue(EqualToOp.eval(false, Boolean.FALSE));
		Assert.assertTrue(EqualToOp.eval(Boolean.TRUE, true));
		Assert.assertTrue(EqualToOp.eval(Boolean.FALSE, false));
		Assert.assertTrue(EqualToOp.eval(true, true));
		Assert.assertTrue(EqualToOp.eval(false, false));
		Assert.assertTrue(EqualToOp.eval(Boolean.TRUE, Boolean.TRUE));
		Assert.assertTrue(EqualToOp.eval(Boolean.FALSE, Boolean.FALSE));
		
		Assert.assertFalse(EqualToOp.eval(true, Boolean.FALSE));
		Assert.assertFalse(EqualToOp.eval(false, Boolean.TRUE));
		Assert.assertFalse(EqualToOp.eval(Boolean.TRUE, false));
		Assert.assertFalse(EqualToOp.eval(Boolean.FALSE, true));
		Assert.assertFalse(EqualToOp.eval(true, false));
		Assert.assertFalse(EqualToOp.eval(false, true));
		Assert.assertFalse(EqualToOp.eval(Boolean.TRUE, Boolean.FALSE));
		Assert.assertFalse(EqualToOp.eval(Boolean.FALSE, Boolean.TRUE));
		
		Byte b1 = randomNonNullByte();
		Byte b2 = new Byte(b1.byteValue());
		Assert.assertTrue(EqualToOp.eval(b1, b2));
		Assert.assertFalse(EqualToOp.eval(b1, randomByteNonEqual(b1)));
		
		Short s1 = randomNonNullShort();
		Short s2 = new Short(s1.shortValue());
		Assert.assertTrue(s1 != s2);
		Assert.assertTrue(EqualToOp.eval(s1, s2));
		Assert.assertFalse(EqualToOp.eval(s1, randomShortNonEqual(s1)));
		
		Integer n1 = randomNonNullInt();
		Integer n2 = new Integer(n1.intValue());
		Assert.assertTrue(n1 != n2);
		Assert.assertTrue(EqualToOp.eval(n1, n2));
		Assert.assertFalse(EqualToOp.eval(n1, randomIntNonEqual(n1)));
		
		Long L1 = randomNonNullLong();
		Long L2 = new Long(L1.longValue());
		Assert.assertTrue(L1 != L2);
		Assert.assertTrue(EqualToOp.eval(L1, L2));
		Assert.assertFalse(EqualToOp.eval(L1, randomLongNonEqual(L1)));
		
		Float f1 = randomNonNullFloat();
		Float f2 = new Float(f1.floatValue());
		Assert.assertTrue(f1 != f2);
		Assert.assertTrue(EqualToOp.eval(f1, f2));
		Assert.assertFalse(EqualToOp.eval(f1, randomFloatNonEqual(f1)));
		
		Double d1 = randomNonNullDouble();
		Double d2 = new Double(d1.doubleValue());
		Assert.assertTrue(d1 != d2);
		Assert.assertTrue(EqualToOp.eval(d1, d2));
		Assert.assertFalse(EqualToOp.eval(d1, randomDoubleNonEqual(d1)));
		
	}
	
	@Test
	public void testNotEqual() {
		
		Assert.assertNull(NotEqualOp.eval((Boolean)null, true));
		Assert.assertNull(NotEqualOp.eval((Boolean)null, false));
		Assert.assertNull(NotEqualOp.eval(true, (Boolean)null));
		Assert.assertNull(NotEqualOp.eval(false, (Boolean)null));
		
		Assert.assertNull(NotEqualOp.eval((Byte)null, randomByte()));
		Assert.assertNull(NotEqualOp.eval(randomByte(), (Byte)null));
		Assert.assertNull(NotEqualOp.eval((Short)null, randomByte()));
		Assert.assertNull(NotEqualOp.eval((Short)null, randomShort()));
		Assert.assertNull(NotEqualOp.eval(randomByte(), (Short)null));
		Assert.assertNull(NotEqualOp.eval(randomShort(), (Short)null));
		Assert.assertNull(NotEqualOp.eval((Integer)null, randomByte()));
		Assert.assertNull(NotEqualOp.eval((Integer)null, randomShort()));
		Assert.assertNull(NotEqualOp.eval((Integer)null, randomInt()));
		Assert.assertNull(NotEqualOp.eval(randomByte(), (Integer)null));
		Assert.assertNull(NotEqualOp.eval(randomShort(), (Integer)null));
		Assert.assertNull(NotEqualOp.eval(randomInt(), (Integer)null));
		Assert.assertNull(NotEqualOp.eval((Long)null, randomByte()));
		Assert.assertNull(NotEqualOp.eval((Long)null, randomShort()));
		Assert.assertNull(NotEqualOp.eval((Long)null, randomInt()));
		Assert.assertNull(NotEqualOp.eval((Long)null, randomLong()));
		Assert.assertNull(NotEqualOp.eval(randomByte(), (Long)null));
		Assert.assertNull(NotEqualOp.eval(randomShort(), (Long)null));
		Assert.assertNull(NotEqualOp.eval(randomInt(), (Long)null));
		Assert.assertNull(NotEqualOp.eval(randomLong(), (Long)null));
		Assert.assertNull(NotEqualOp.eval((Float)null, randomByte()));
		Assert.assertNull(NotEqualOp.eval((Float)null, randomShort()));
		Assert.assertNull(NotEqualOp.eval((Float)null, randomInt()));
		Assert.assertNull(NotEqualOp.eval((Float)null, randomLong()));
		Assert.assertNull(NotEqualOp.eval((Float)null, randomFloat()));
		Assert.assertNull(NotEqualOp.eval(randomByte(), (Float)null));
		Assert.assertNull(NotEqualOp.eval(randomShort(), (Float)null));
		Assert.assertNull(NotEqualOp.eval(randomInt(), (Float)null));
		Assert.assertNull(NotEqualOp.eval(randomLong(), (Float)null));
		Assert.assertNull(NotEqualOp.eval(randomFloat(), (Float)null));
		Assert.assertNull(NotEqualOp.eval((Double)null, randomByte()));
		Assert.assertNull(NotEqualOp.eval((Double)null, randomShort()));
		Assert.assertNull(NotEqualOp.eval((Double)null, randomInt()));
		Assert.assertNull(NotEqualOp.eval((Double)null, randomLong()));
		Assert.assertNull(NotEqualOp.eval((Double)null, randomFloat()));
		Assert.assertNull(NotEqualOp.eval((Double)null, randomDouble()));
		Assert.assertNull(NotEqualOp.eval(randomByte(), (Double)null));
		Assert.assertNull(NotEqualOp.eval(randomShort(), (Double)null));
		Assert.assertNull(NotEqualOp.eval(randomInt(), (Double)null));
		Assert.assertNull(NotEqualOp.eval(randomLong(), (Double)null));
		Assert.assertNull(NotEqualOp.eval(randomFloat(), (Double)null));
		Assert.assertNull(NotEqualOp.eval(randomDouble(), (Double)null));
		Assert.assertNull(NotEqualOp.eval((String)null, randomString()));
		Assert.assertNull(NotEqualOp.eval(randomString(), (String)null));
		
		Assert.assertFalse(NotEqualOp.eval(true, Boolean.TRUE));
		Assert.assertFalse(NotEqualOp.eval(false, Boolean.FALSE));
		Assert.assertFalse(NotEqualOp.eval(Boolean.TRUE, true));
		Assert.assertFalse(NotEqualOp.eval(Boolean.FALSE, false));
		Assert.assertFalse(NotEqualOp.eval(true, true));
		Assert.assertFalse(NotEqualOp.eval(false, false));
		Assert.assertFalse(NotEqualOp.eval(Boolean.TRUE, Boolean.TRUE));
		Assert.assertFalse(NotEqualOp.eval(Boolean.FALSE, Boolean.FALSE));
		
		Assert.assertTrue(NotEqualOp.eval(true, Boolean.FALSE));
		Assert.assertTrue(NotEqualOp.eval(false, Boolean.TRUE));
		Assert.assertTrue(NotEqualOp.eval(Boolean.TRUE, false));
		Assert.assertTrue(NotEqualOp.eval(Boolean.FALSE, true));
		Assert.assertTrue(NotEqualOp.eval(true, false));
		Assert.assertTrue(NotEqualOp.eval(false, true));
		Assert.assertTrue(NotEqualOp.eval(Boolean.TRUE, Boolean.FALSE));
		Assert.assertTrue(NotEqualOp.eval(Boolean.FALSE, Boolean.TRUE));
		
		Byte b1 = randomNonNullByte();
		Byte b2 = new Byte(b1.byteValue());
		Assert.assertFalse(NotEqualOp.eval(b1, b2));
		Assert.assertTrue(NotEqualOp.eval(b1, randomByteNonEqual(b1)));
		
		Short s1 = randomNonNullShort();
		Short s2 = new Short(s1.shortValue());
		Assert.assertTrue(s1 != s2);
		Assert.assertFalse(NotEqualOp.eval(s1, s2));
		Assert.assertTrue(NotEqualOp.eval(s1, randomShortNonEqual(s1)));
		
		Integer n1 = randomNonNullInt();
		Integer n2 = new Integer(n1.intValue());
		Assert.assertTrue(n1 != n2);
		Assert.assertFalse(NotEqualOp.eval(n1, n2));
		Assert.assertTrue(NotEqualOp.eval(n1, randomIntNonEqual(n1)));
		
		Long L1 = randomNonNullLong();
		Long L2 = new Long(L1.longValue());
		Assert.assertTrue(L1 != L2);
		Assert.assertFalse(NotEqualOp.eval(L1, L2));
		Assert.assertTrue(NotEqualOp.eval(L1, randomLongNonEqual(L1)));
		
		Float f1 = randomNonNullFloat();
		Float f2 = new Float(f1.floatValue());
		Assert.assertTrue(f1 != f2);
		Assert.assertFalse(NotEqualOp.eval(f1, f2));
		Assert.assertTrue(NotEqualOp.eval(f1, randomFloatNonEqual(f1)));
		
		Double d1 = randomNonNullDouble();
		Double d2 = new Double(d1.doubleValue());
		Assert.assertTrue(d1 != d2);
		Assert.assertFalse(NotEqualOp.eval(d1, d2));
		Assert.assertTrue(NotEqualOp.eval(d1, randomDoubleNonEqual(d1)));

	}
	
	@Test
	public void testCompare() {
		Assert.assertNull(GreaterThanOp.eval((Byte)null, randomByte()));
		Assert.assertNull(GreaterThanOp.eval(randomByte(), (Byte)null));
		Assert.assertNull(GreaterThanOp.eval((Short)null, randomByte()));
		Assert.assertNull(GreaterThanOp.eval((Short)null, randomShort()));
		Assert.assertNull(GreaterThanOp.eval(randomByte(), (Short)null));
		Assert.assertNull(GreaterThanOp.eval(randomShort(), (Short)null));
		Assert.assertNull(GreaterThanOp.eval((Integer)null, randomByte()));
		Assert.assertNull(GreaterThanOp.eval((Integer)null, randomShort()));
		Assert.assertNull(GreaterThanOp.eval((Integer)null, randomInt()));
		Assert.assertNull(GreaterThanOp.eval(randomByte(), (Integer)null));
		Assert.assertNull(GreaterThanOp.eval(randomShort(), (Integer)null));
		Assert.assertNull(GreaterThanOp.eval(randomInt(), (Integer)null));
		Assert.assertNull(GreaterThanOp.eval((Long)null, randomByte()));
		Assert.assertNull(GreaterThanOp.eval((Long)null, randomShort()));
		Assert.assertNull(GreaterThanOp.eval((Long)null, randomInt()));
		Assert.assertNull(GreaterThanOp.eval((Long)null, randomLong()));
		Assert.assertNull(GreaterThanOp.eval(randomByte(), (Long)null));
		Assert.assertNull(GreaterThanOp.eval(randomShort(), (Long)null));
		Assert.assertNull(GreaterThanOp.eval(randomInt(), (Long)null));
		Assert.assertNull(GreaterThanOp.eval(randomLong(), (Long)null));
		Assert.assertNull(GreaterThanOp.eval((Float)null, randomByte()));
		Assert.assertNull(GreaterThanOp.eval((Float)null, randomShort()));
		Assert.assertNull(GreaterThanOp.eval((Float)null, randomInt()));
		Assert.assertNull(GreaterThanOp.eval((Float)null, randomLong()));
		Assert.assertNull(GreaterThanOp.eval((Float)null, randomFloat()));
		Assert.assertNull(GreaterThanOp.eval(randomByte(), (Float)null));
		Assert.assertNull(GreaterThanOp.eval(randomShort(), (Float)null));
		Assert.assertNull(GreaterThanOp.eval(randomInt(), (Float)null));
		Assert.assertNull(GreaterThanOp.eval(randomLong(), (Float)null));
		Assert.assertNull(GreaterThanOp.eval(randomFloat(), (Float)null));
		Assert.assertNull(GreaterThanOp.eval((Double)null, randomByte()));
		Assert.assertNull(GreaterThanOp.eval((Double)null, randomShort()));
		Assert.assertNull(GreaterThanOp.eval((Double)null, randomInt()));
		Assert.assertNull(GreaterThanOp.eval((Double)null, randomLong()));
		Assert.assertNull(GreaterThanOp.eval((Double)null, randomFloat()));
		Assert.assertNull(GreaterThanOp.eval((Double)null, randomDouble()));
		Assert.assertNull(GreaterThanOp.eval(randomByte(), (Double)null));
		Assert.assertNull(GreaterThanOp.eval(randomShort(), (Double)null));
		Assert.assertNull(GreaterThanOp.eval(randomInt(), (Double)null));
		Assert.assertNull(GreaterThanOp.eval(randomLong(), (Double)null));
		Assert.assertNull(GreaterThanOp.eval(randomFloat(), (Double)null));
		Assert.assertNull(GreaterThanOp.eval(randomDouble(), (Double)null));
		Assert.assertNull(GreaterThanOp.eval((String)null, randomString()));
		Assert.assertNull(GreaterThanOp.eval(randomString(), (String)null));
		
		Assert.assertNull(GreaterThanOrEqualOp.eval((Byte)null, randomByte()));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomByte(), (Byte)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Short)null, randomByte()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Short)null, randomShort()));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomByte(), (Short)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomShort(), (Short)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Integer)null, randomByte()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Integer)null, randomShort()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Integer)null, randomInt()));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomByte(), (Integer)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomShort(), (Integer)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomInt(), (Integer)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Long)null, randomByte()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Long)null, randomShort()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Long)null, randomInt()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Long)null, randomLong()));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomByte(), (Long)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomShort(), (Long)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomInt(), (Long)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomLong(), (Long)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Float)null, randomByte()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Float)null, randomShort()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Float)null, randomInt()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Float)null, randomLong()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Float)null, randomFloat()));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomByte(), (Float)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomShort(), (Float)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomInt(), (Float)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomLong(), (Float)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomFloat(), (Float)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Double)null, randomByte()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Double)null, randomShort()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Double)null, randomInt()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Double)null, randomLong()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Double)null, randomFloat()));
		Assert.assertNull(GreaterThanOrEqualOp.eval((Double)null, randomDouble()));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomByte(), (Double)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomShort(), (Double)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomInt(), (Double)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomLong(), (Double)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomFloat(), (Double)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomDouble(), (Double)null));
		Assert.assertNull(GreaterThanOrEqualOp.eval((String)null, randomString()));
		Assert.assertNull(GreaterThanOrEqualOp.eval(randomString(), (String)null));
		
		Assert.assertNull(LessThanOp.eval((Byte)null, randomByte()));
		Assert.assertNull(LessThanOp.eval(randomByte(), (Byte)null));
		Assert.assertNull(LessThanOp.eval((Short)null, randomByte()));
		Assert.assertNull(LessThanOp.eval((Short)null, randomShort()));
		Assert.assertNull(LessThanOp.eval(randomByte(), (Short)null));
		Assert.assertNull(LessThanOp.eval(randomShort(), (Short)null));
		Assert.assertNull(LessThanOp.eval((Integer)null, randomByte()));
		Assert.assertNull(LessThanOp.eval((Integer)null, randomShort()));
		Assert.assertNull(LessThanOp.eval((Integer)null, randomInt()));
		Assert.assertNull(LessThanOp.eval(randomByte(), (Integer)null));
		Assert.assertNull(LessThanOp.eval(randomShort(), (Integer)null));
		Assert.assertNull(LessThanOp.eval(randomInt(), (Integer)null));
		Assert.assertNull(LessThanOp.eval((Long)null, randomByte()));
		Assert.assertNull(LessThanOp.eval((Long)null, randomShort()));
		Assert.assertNull(LessThanOp.eval((Long)null, randomInt()));
		Assert.assertNull(LessThanOp.eval((Long)null, randomLong()));
		Assert.assertNull(LessThanOp.eval(randomByte(), (Long)null));
		Assert.assertNull(LessThanOp.eval(randomShort(), (Long)null));
		Assert.assertNull(LessThanOp.eval(randomInt(), (Long)null));
		Assert.assertNull(LessThanOp.eval(randomLong(), (Long)null));
		Assert.assertNull(LessThanOp.eval((Float)null, randomByte()));
		Assert.assertNull(LessThanOp.eval((Float)null, randomShort()));
		Assert.assertNull(LessThanOp.eval((Float)null, randomInt()));
		Assert.assertNull(LessThanOp.eval((Float)null, randomLong()));
		Assert.assertNull(LessThanOp.eval((Float)null, randomFloat()));
		Assert.assertNull(LessThanOp.eval(randomByte(), (Float)null));
		Assert.assertNull(LessThanOp.eval(randomShort(), (Float)null));
		Assert.assertNull(LessThanOp.eval(randomInt(), (Float)null));
		Assert.assertNull(LessThanOp.eval(randomLong(), (Float)null));
		Assert.assertNull(LessThanOp.eval(randomFloat(), (Float)null));
		Assert.assertNull(LessThanOp.eval((Double)null, randomByte()));
		Assert.assertNull(LessThanOp.eval((Double)null, randomShort()));
		Assert.assertNull(LessThanOp.eval((Double)null, randomInt()));
		Assert.assertNull(LessThanOp.eval((Double)null, randomLong()));
		Assert.assertNull(LessThanOp.eval((Double)null, randomFloat()));
		Assert.assertNull(LessThanOp.eval((Double)null, randomDouble()));
		Assert.assertNull(LessThanOp.eval(randomByte(), (Double)null));
		Assert.assertNull(LessThanOp.eval(randomShort(), (Double)null));
		Assert.assertNull(LessThanOp.eval(randomInt(), (Double)null));
		Assert.assertNull(LessThanOp.eval(randomLong(), (Double)null));
		Assert.assertNull(LessThanOp.eval(randomFloat(), (Double)null));
		Assert.assertNull(LessThanOp.eval(randomDouble(), (Double)null));
		Assert.assertNull(LessThanOp.eval((String)null, randomString()));
		Assert.assertNull(LessThanOp.eval(randomString(), (String)null));
		
		Assert.assertNull(LessThanOrEqualOp.eval((Byte)null, randomByte()));
		Assert.assertNull(LessThanOrEqualOp.eval(randomByte(), (Byte)null));
		Assert.assertNull(LessThanOrEqualOp.eval((Short)null, randomByte()));
		Assert.assertNull(LessThanOrEqualOp.eval((Short)null, randomShort()));
		Assert.assertNull(LessThanOrEqualOp.eval(randomByte(), (Short)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomShort(), (Short)null));
		Assert.assertNull(LessThanOrEqualOp.eval((Integer)null, randomByte()));
		Assert.assertNull(LessThanOrEqualOp.eval((Integer)null, randomShort()));
		Assert.assertNull(LessThanOrEqualOp.eval((Integer)null, randomInt()));
		Assert.assertNull(LessThanOrEqualOp.eval(randomByte(), (Integer)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomShort(), (Integer)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomInt(), (Integer)null));
		Assert.assertNull(LessThanOrEqualOp.eval((Long)null, randomByte()));
		Assert.assertNull(LessThanOrEqualOp.eval((Long)null, randomShort()));
		Assert.assertNull(LessThanOrEqualOp.eval((Long)null, randomInt()));
		Assert.assertNull(LessThanOrEqualOp.eval((Long)null, randomLong()));
		Assert.assertNull(LessThanOrEqualOp.eval(randomByte(), (Long)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomShort(), (Long)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomInt(), (Long)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomLong(), (Long)null));
		Assert.assertNull(LessThanOrEqualOp.eval((Float)null, randomByte()));
		Assert.assertNull(LessThanOrEqualOp.eval((Float)null, randomShort()));
		Assert.assertNull(LessThanOrEqualOp.eval((Float)null, randomInt()));
		Assert.assertNull(LessThanOrEqualOp.eval((Float)null, randomLong()));
		Assert.assertNull(LessThanOrEqualOp.eval((Float)null, randomFloat()));
		Assert.assertNull(LessThanOrEqualOp.eval(randomByte(), (Float)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomShort(), (Float)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomInt(), (Float)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomLong(), (Float)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomFloat(), (Float)null));
		Assert.assertNull(LessThanOrEqualOp.eval((Double)null, randomByte()));
		Assert.assertNull(LessThanOrEqualOp.eval((Double)null, randomShort()));
		Assert.assertNull(LessThanOrEqualOp.eval((Double)null, randomInt()));
		Assert.assertNull(LessThanOrEqualOp.eval((Double)null, randomLong()));
		Assert.assertNull(LessThanOrEqualOp.eval((Double)null, randomFloat()));
		Assert.assertNull(LessThanOrEqualOp.eval((Double)null, randomDouble()));
		Assert.assertNull(LessThanOrEqualOp.eval(randomByte(), (Double)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomShort(), (Double)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomInt(), (Double)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomLong(), (Double)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomFloat(), (Double)null));
		Assert.assertNull(LessThanOrEqualOp.eval(randomDouble(), (Double)null));
		Assert.assertNull(LessThanOrEqualOp.eval((String)null, randomString()));
		Assert.assertNull(LessThanOrEqualOp.eval(randomString(), (String)null));
		
		Byte b1 = randomNonNullByte();
		Byte b2 = randomNonNullByte();
		Byte b11 = new Byte(b1.byteValue());
		Assert.assertTrue(GreaterThanOp.eval(b1, b2) == b1 > b2);
		Assert.assertFalse(GreaterThanOp.eval(b1, b11));
		Assert.assertTrue(GreaterThanOrEqualOp.eval(b1, b2) == b1 >= b2);
		Assert.assertTrue(GreaterThanOrEqualOp.eval(b1, b11));
		Assert.assertTrue(LessThanOp.eval(b1, b2) == b1 < b2);
		Assert.assertFalse(LessThanOp.eval(b1, b11));
		Assert.assertTrue(LessThanOrEqualOp.eval(b1, b2) == b1 <= b2);
		Assert.assertTrue(LessThanOrEqualOp.eval(b1, b11));
		
		Short s1 = randomNonNullShort();
		Short s2 = randomNonNullShort();
		Short s11 = new Short(s1.shortValue());
		Assert.assertTrue(GreaterThanOp.eval(s1, s2) == s1 > s2);
		Assert.assertFalse(GreaterThanOp.eval(s1, s11));
		Assert.assertTrue(GreaterThanOrEqualOp.eval(s1, s2) == s1 >= s2);
		Assert.assertTrue(GreaterThanOrEqualOp.eval(s1, s11));
		Assert.assertTrue(LessThanOp.eval(s1, s2) == s1 < s2);
		Assert.assertFalse(LessThanOp.eval(s1, s11));
		Assert.assertTrue(LessThanOrEqualOp.eval(s1, s2) == s1 <= s2);
		Assert.assertTrue(LessThanOrEqualOp.eval(s1, s11));
		
		Integer n1 = randomNonNullInt();
		Integer n2 = randomNonNullInt();
		Integer n11 = new Integer(n1.intValue());
		Assert.assertTrue(GreaterThanOp.eval(n1, n2) == n1 > n2);
		Assert.assertFalse(GreaterThanOp.eval(n1, n11));
		Assert.assertTrue(GreaterThanOrEqualOp.eval(n1, n2) == n1 >= n2);
		Assert.assertTrue(GreaterThanOrEqualOp.eval(n1, n11));
		Assert.assertTrue(LessThanOp.eval(n1, n2) == n1 < n2);
		Assert.assertFalse(LessThanOp.eval(n1, n11));
		Assert.assertTrue(LessThanOrEqualOp.eval(n1, n2) == n1 <= n2);
		Assert.assertTrue(LessThanOrEqualOp.eval(n1, n11));
		
		Long L1 = randomNonNullLong();
		Long L2 = randomNonNullLong();
		Long L11 = new Long(L1.longValue());
		Assert.assertTrue(GreaterThanOp.eval(L1, L2) == L1 > L2);
		Assert.assertFalse(GreaterThanOp.eval(L1, L11));
		Assert.assertTrue(GreaterThanOrEqualOp.eval(L1, L2) == L1 >= L2);
		Assert.assertTrue(GreaterThanOrEqualOp.eval(L1, L11));
		Assert.assertTrue(LessThanOp.eval(L1, L2) == L1 < L2);
		Assert.assertFalse(LessThanOp.eval(L1, L11));
		Assert.assertTrue(LessThanOrEqualOp.eval(L1, L2) == L1 <= L2);
		Assert.assertTrue(LessThanOrEqualOp.eval(L1, L11));
		
		Float f1 = randomNonNullFloat();
		Float f2 = randomNonNullFloat();
		Float f11 = new Float(f1.floatValue());
		Assert.assertTrue(GreaterThanOp.eval(f1, f2) == f1 > f2);
		Assert.assertFalse(GreaterThanOp.eval(f1, f11));
		Assert.assertTrue(GreaterThanOrEqualOp.eval(f1, f2) == f1 >= f2);
		Assert.assertTrue(GreaterThanOrEqualOp.eval(f1, f11));
		Assert.assertTrue(LessThanOp.eval(f1, f2) == f1 < f2);
		Assert.assertFalse(LessThanOp.eval(f1, f11));
		Assert.assertTrue(LessThanOrEqualOp.eval(f1, f2) == f1 <= f2);
		Assert.assertTrue(LessThanOrEqualOp.eval(f1, f11));
		
		Double d1 = randomNonNullDouble();
		Double d2 = randomNonNullDouble();
		Double d11 = new Double(d1.doubleValue());
		Assert.assertTrue(GreaterThanOp.eval(d1, d2) == d1 > d2);
		Assert.assertFalse(GreaterThanOp.eval(d1, d11));
		Assert.assertTrue(GreaterThanOrEqualOp.eval(d1, d2) == d1 >= d2);
		Assert.assertTrue(GreaterThanOrEqualOp.eval(d1, d11));
		Assert.assertTrue(LessThanOp.eval(d1, d2) == d1 < d2);
		Assert.assertFalse(LessThanOp.eval(d1, d11));
		Assert.assertTrue(LessThanOrEqualOp.eval(d1, d2) == d1 <= d2);
		Assert.assertTrue(LessThanOrEqualOp.eval(d1, d11));
	}
	
	private Byte randomByte() {
		return _rand.nextInt(2) == 0 ? null : randomNonNullByte();
	}
	
	private Byte randomNonNullByte() {
		return (byte)(_rand.nextInt(256) - 128);
	}
	
	private Byte randomNonZeroByte() {
		byte b = 0;
		while (b == 0) {
			b = randomNonNullByte();
		}
		return b;
	}
	
	private Byte randomByteNonEqual(byte compare) {
		byte b = compare;
		while (b == compare) {
			b = randomNonNullByte();
		}
		return b;
	}
	
	private Short randomShort() {
		return _rand.nextInt(2) == 0 ? null : randomNonNullShort();
	}
	
	private Short randomNonNullShort() {
		return (short)(_rand.nextInt(65536) - 32768);
	}
	
	private Short randomNonZeroShort() {
		short s = 0;
		while (s == 0) {
			s = randomNonNullShort();
		}
		return s;
	}
	
	private Short randomShortNonEqual(short compare) {
		short s = compare;
		while (s == compare) {
			s = randomNonNullShort();
		}
		return s;
	}
	
	private Integer randomInt() {
		return _rand.nextInt(2) == 0 ? null : randomNonNullInt();
	}
	
	private Integer randomNonNullInt() {
		return _rand.nextInt();
	}
	
	private Integer randomNonZeroInt() {
		int n = 0;
		while (n == 0) {
			n = randomNonNullInt();
		}
		return n;
	}
	
	private Integer randomIntNonEqual(int compare) {
		int n = compare;
		while (n == compare) {
			n = randomNonNullInt();
		}
		return n;
	}
	
	private Long randomLong() {
		return _rand.nextInt(2) == 0 ? null : randomNonNullLong();
	}
	
	private Long randomNonNullLong() {
		return _rand.nextLong();
	}
	
	private Long randomNonZeroLong() {
		long n = 0;
		while (n == 0) {
			n = randomNonNullLong();
		}
		return n;
	}
	
	private Long randomLongNonEqual(long compare) {
		long n = compare;
		while (n == compare) {
			n = randomNonNullLong();
		}
		return n;
	}
	
	private Float randomFloat() {
		return _rand.nextInt(2) == 0 ? null : randomNonNullFloat();
	}
	
	private Float randomNonNullFloat() {
		return _rand.nextFloat();
	}
	
	private Float randomNonZeroFloat() {
		float n = 0;
		while (n == 0) {
			n = randomNonNullFloat();
		}
		return n;
	}
	
	private Float randomFloatNonEqual(float compare) {
		float n = compare;
		while (n == compare) {
			n = randomNonNullFloat();
		}
		return n;
	}
	
	private Double randomDouble() {
		return _rand.nextInt(2) == 0 ? null : randomNonNullDouble();
	}
	
	private Double randomNonNullDouble() {
		return _rand.nextDouble();
	}
	
	private Double randomNonZeroDouble() {
		double n = 0;
		while (n == 0) {
			n = randomNonNullDouble();
		}
		return n;
	}
	
	private Double randomDoubleNonEqual(double compare) {
		double n = compare;
		while (n == compare) {
			n = randomNonNullFloat();
		}
		return n;
	}
	
	private String randomString() {
		return _rand.nextInt(2) == 0 ? null : randomNonNullString();
	}
	
	private String randomNonNullString() {
		int len = _rand.nextInt(256);
		StringBuilder sb = new StringBuilder(len);
		for(int n = 0; n < len; n +=1) {
			sb.append((char)(1+_rand.nextInt(127)));
			
		}
		return sb.toString();
	}
}
