package com.pplive.pike.base;

import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.PikeSqlCompiler;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;

public class PeriodTest  extends BaseUnitTest {

    @Test
    public void testParsePeriod(){

        Assert.assertTrue(Period.parse("1s").equals(Period.secondsOf(1)));
        Assert.assertTrue(Period.parse("1S").equals(Period.secondsOf(1)));
        Assert.assertTrue(Period.parse("11s").equals(Period.secondsOf(11)));
        Assert.assertTrue(Period.parse("11s").equals(Period.secondsOf(11)));
        Assert.assertTrue(Period.parse("1m").equals(Period.secondsOf(1*60)));
        Assert.assertTrue(Period.parse("1M").equals(Period.secondsOf(1*60)));
        Assert.assertTrue(Period.parse("100m").equals(Period.secondsOf(100*60)));
        Assert.assertTrue(Period.parse("100M").equals(Period.secondsOf(100*60)));
        Assert.assertTrue(Period.parse("1h").equals(Period.secondsOf(1*3600)));
        Assert.assertTrue(Period.parse("1H").equals(Period.secondsOf(1*3600)));
        Assert.assertTrue(Period.parse("123h").equals(Period.secondsOf(123*3600)));
        Assert.assertTrue(Period.parse("123H").equals(Period.secondsOf(123*3600)));
        Assert.assertTrue(Period.parse("1d").equals(Period.secondsOf(1*3600*24)));
        Assert.assertTrue(Period.parse("1D").equals(Period.secondsOf(1*3600*24)));
        Assert.assertTrue(Period.parse("99d").equals(Period.secondsOf(99*3600*24)));
        Assert.assertTrue(Period.parse("99D").equals(Period.secondsOf(99 * 3600 * 24)));
    }

    @Test
    public void testPeriodBegin() {
        Period p = Period.secondsOf(60);

        Calendar t1 = Calendar.getInstance();
        t1.set(Calendar.MILLISECOND, 999);
        t1.set(Calendar.SECOND, 59);

        Calendar t2 = Calendar.getInstance();
        t2.set(Calendar.MILLISECOND, 0);
        t2.set(Calendar.SECOND, 0);
        t2.add(Calendar.MINUTE, 1);

        Calendar begin = p.periodBegin(t1, 0);
        assert (begin.equals(p.periodBegin(t2, 0)) == false);
        assert (begin.equals(p.periodBegin(t2, 1)));

        t2.set(Calendar.MILLISECOND, 100);
        assert (begin.equals(p.periodBegin(t2, 100)) == false);
        assert (begin.equals(p.periodBegin(t2, 101)));

        t2.set(Calendar.MILLISECOND, 0);
        t2.set(Calendar.SECOND, 4);
        assert (begin.equals(p.periodBegin(t2, 4000)) == false);
        assert (begin.equals(p.periodBegin(t2, 4001)));
    }
}
