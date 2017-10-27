package com.pplive.pike.function;

import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.ConstantExpression;
import com.pplive.pike.expression.MockExpression;
import com.pplive.pike.function.builtin.HyperLoglogCount;
import com.pplive.pike.function.builtin.HyperLoglogCountState;
import com.pplive.pike.function.builtin.LoglogAdaptiveCount;
import com.pplive.pike.function.builtin.LoglogAdaptiveCountState;
import com.pplive.pike.generator.trident.ICombinable;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Created by jiatingjin on 2017/10/24.
 */
public class CountTest extends BaseUnitTest {



    @Test
    public void testLoglogAdaptiveCount() {
        ArrayList<AbstractExpression> paramExprs = new ArrayList<AbstractExpression>(1);
        paramExprs.add(new MockExpression());
        ICombinable<LoglogAdaptiveCountState, Long> count =LoglogAdaptiveCount.createCombinable(5, paramExprs);

        LoglogAdaptiveCountState state = count.init();

        state.combineNonNull(state, count.evalToCombinable(null));

        state.combineNonNull(state, count.evalToCombinable(null));

        state.combineNonNull(state, count.evalToCombinable(null));

        System.out.println(count.finish(state));

    }

    @Test
    public void testHyperLoglogCount() {
        ArrayList<AbstractExpression> paramExprs = new ArrayList<AbstractExpression>(1);
        paramExprs.add(new MockExpression());
        ICombinable<HyperLoglogCountState, Long> count = HyperLoglogCount.createCombinable(5, paramExprs);

        HyperLoglogCountState left = count.init();

        System.out.println(left.cardinality());

        HyperLoglogCountState right = count.init();

        right.combineNonNull(right, count.evalToCombinable(null));

        right.combineNonNull(right, count.evalToCombinable(null));

        right.combineNonNull(right, count.evalToCombinable(null));

        right.combineNonNull(right, count.evalToCombinable(null));

        System.out.println(right.cardinality());

        System.out.println(count.combine(left, right).cardinality());

    }
}
