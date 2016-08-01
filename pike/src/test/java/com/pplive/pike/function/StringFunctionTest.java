package com.pplive.pike.function;

import junit.framework.Assert;

import org.junit.Test;

import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.function.builtin.Str;
import com.pplive.pike.function.builtin.Str.Concat;
import com.pplive.pike.function.builtin.Str.ConcatTriple;

public class StringFunctionTest extends BaseUnitTest {
    @Test
    public void testConcat() {
        new Str.Concat();
        Assert.assertEquals(Concat.evaluate("a", "b"), "ab");
        Assert.assertEquals(Concat.evaluate("", "b"), "b");
        Assert.assertEquals(Concat.evaluate("a", ""), "a");
        Assert.assertEquals(Concat.evaluate("a", null), "a");
        Assert.assertEquals(Concat.evaluate(null, "a"), "a");
        Assert.assertEquals(Concat.evaluate(null, null), "");
        Assert.assertEquals(Concat.evaluate("", null), "");
        Assert.assertEquals(Concat.evaluate(null, ""), "");
        Assert.assertEquals(Concat.evaluate("", ""), "");

        Assert.assertEquals(Concat.evaluate("a", null), "a");
        Assert.assertEquals(Concat.evaluate(null, "a"), "a");
    }

    @Test
    public void testConcatTriple() {
        Assert.assertEquals(ConcatTriple.evaluate("a", "_", "b"), "a_b");
        Assert.assertEquals(ConcatTriple.evaluate("b", "_", "a"), "b_a");
        Assert.assertEquals(ConcatTriple.evaluate("b", "|", "a"), "b|a");
        Assert.assertEquals(ConcatTriple.evaluate("a", "", "b"), "ab");
        Assert.assertEquals(ConcatTriple.evaluate("a", null, "b"), "ab");
        Assert.assertEquals(ConcatTriple.evaluate("", "_", "b"), "_b");
        Assert.assertEquals(ConcatTriple.evaluate("a", "", ""), "a");
        Assert.assertEquals(ConcatTriple.evaluate("a", null, ""), "a");
        Assert.assertEquals(ConcatTriple.evaluate(null, "_", "a"), "_a");
        Assert.assertEquals(ConcatTriple.evaluate(null, null, null), "");
        Assert.assertEquals(ConcatTriple.evaluate("", null, ""), "");
        Assert.assertEquals(ConcatTriple.evaluate(null, "", ""), "");
        Assert.assertEquals(ConcatTriple.evaluate("", "", ""), "");

        Assert.assertEquals(ConcatTriple.evaluate("a", "a", null), "aa");
        Assert.assertEquals(ConcatTriple.evaluate(null, "b", "a"), "ba");
    }
}
