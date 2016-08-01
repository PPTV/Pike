package com.pplive.pike.function.builtin;

import java.io.Serializable;
import java.util.Arrays;

import com.clearspring.analytics.hash.Lookup3Hash;
import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import com.clearspring.analytics.stream.cardinality.AdaptiveCounting.Builder;
import com.clearspring.analytics.util.IBuilder;

// AdaptiveCounting is not Serializable, so we have to create a wrapper
class SerializableAdaptiveCounting extends AdaptiveCounting implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public SerializableAdaptiveCounting() {
		super(16);
	}
	
	public SerializableAdaptiveCounting(AdaptiveCounting other) {
		super(other.getBytes());
	}

    public boolean offer(Object[] objs)
    {
    	if (objs.length == 1){
    		return offer(String.valueOf(objs[0]));
    	}
    	StringBuilder sb = new StringBuilder(objs.length * 10);
        for(int n = 0; n < objs.length; n += 1){
       		sb.append(objs[n]);
        }
        return offer(sb.toString());
    }
}

final class LoglogAdaptiveCountState implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public boolean isOneData() { return _counting == null; }
	private final Object[] _objs;

	private final SerializableAdaptiveCounting _counting;
	
	public LoglogAdaptiveCountState(SerializableAdaptiveCounting counting) {
		assert counting != null;
		this._counting = counting;
		this._objs = null;
	}

	public LoglogAdaptiveCountState(Object[] objs) {
		this._counting = null;
		assert objs != null;
		this._objs = objs;
	}
	
	public long cardinality() {
		return isOneData() ? 1L : this._counting.cardinality();
	}
	
	public static LoglogAdaptiveCountState combineNonNull(LoglogAdaptiveCountState left, LoglogAdaptiveCountState right) {
		if (left.isOneData() == false && right.isOneData()) {
			left._counting.offer(right._objs);
			return left;
		}
		else if (left.isOneData() && right.isOneData() == false) {
			right._counting.offer(left._objs);
			return right;
		}
		else if (left.isOneData() == false && right.isOneData() == false) {
			assert left._counting.sizeof() == right._counting.sizeof();
			try {
				AdaptiveCounting c = AdaptiveCounting.mergeEstimators(left._counting, right._counting);
				return new LoglogAdaptiveCountState(new SerializableAdaptiveCounting(c));
			}
			catch(CardinalityMergeException e) {
				throw new IllegalStateException(e);
			}
		}
		else {
			assert false;
			throw new IllegalStateException("should never happen: LoglogAdaptiveCountState combine(): left and right both are one data");
		}
	}
}


//----------------------------------------------------
//copy from Stream-Lib, have to add Serializable
/**
 * <p>
 * Based on the adaptive counting approach of:<br/>
 * <i>Fast and Accurate Traffic Matrix Measurement Using Adaptive Cardinality Counting</i><br>
 * by:  Cai, Pan, Kwok, and Hwang
 * </p>
 *
 * TODO: use 5 bits/bucket instead of 8 (37.5% size reduction)<br/>
 * TODO: super-LogLog optimizations
 *
 */
class AdaptiveCounting extends LogLog implements Serializable
{
	private static final long serialVersionUID = 1L;
    public AdaptiveCounting() { super(); }
    
    /**
     * Number of empty buckets
     */
    protected int b_e;

    /**
     * Switching empty bucket ratio
     */
    protected final double B_s = 0.051;

    public AdaptiveCounting(int k)
    {
        super(k);
        b_e = m;
    }

    public AdaptiveCounting(byte[] M)
    {
        super(M);

        for(byte b : M)
        {
            if(b == 0) b_e++;
        }
    }

    @Override
    public boolean offer(Object o)
    {
        boolean modified = false;

        long x = Lookup3Hash.lookup3ycs64(o.toString());
        int j = (int) (x >>> (Long.SIZE - k));
        byte r = (byte)(Long.numberOfLeadingZeros( (x << k) | (1<<(k-1)) )+1);
        if(M[j] < r)
        {
            Rsum += r-M[j];
            if(M[j] == 0) b_e--;
            M[j] = r;
            modified = true;
        }

        return modified;
    }

    @Override
    public long cardinality()
    {
        double B = (b_e/(double)m);
        if( B >= B_s )
        {
            return (long)java.lang.Math.round(-m*java.lang.Math.log(B));
        }

        return super.cardinality();
    }


    /**
     * Computes the position of the first set bit of the last Long.SIZE-k bits
     *
     * @return Long.SIZE-k if the last k bits are all zero
     */
    protected static byte rho(long x, int k)
    {
        return (byte)(Long.numberOfLeadingZeros( (x << k) | (1<<(k-1)) )+1);
    }

    /**
     * @return this if estimators is null or no arguments are passed
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be instances of LogLog of the same size)
     */
    @Override
    public ICardinality merge(ICardinality... estimators) throws LogLogMergeException
    {
        LogLog res = (LogLog)super.merge(estimators);
        return new AdaptiveCounting(res.M);
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be the same size)
     */
    public static AdaptiveCounting mergeEstimators(LogLog... estimators) throws LogLogMergeException
    {
        if(estimators == null || estimators.length == 0)
        {
            return null;
        }
        return (AdaptiveCounting)estimators[0].merge(Arrays.copyOfRange(estimators, 1, estimators.length));
    }

    public static class Builder implements IBuilder<ICardinality>, Serializable
    {
        private static final long serialVersionUID = 2205437102378081334L;

        protected final int k;

        public Builder()
        {
            this(16);
        }

        public Builder(int k)
        {
            this.k = k;
        }

        @Override
        public AdaptiveCounting build()
        {
            return new AdaptiveCounting(k);
        }

        @Override
        public int sizeof()
        {
            return 1 << k;
        }

        /**
         * <p>
         * For cardinalities less than 4.25M, obyCount provides a LinearCounting Builder
         * (see LinearCounting.Builder.onePercentError() ) using only the
         * space required to provide estimates within 1% of the actual cardinality,
         * up to ~65k.
         * </p>
         * <p>
         * For cardinalities greater than 4.25M, an AdaptiveCounting builder is returned
         * that allocates ~65KB and provides estimates with a Gaussian error distribution
         * with an average error of 0.5% and a standard deviation of 0.5%
         * </p>
         * @param maxCardinality
         * @throws IllegalArgumentException if maxCardinality is not a positive integer
         *
         * @see LinearCounting.Builder#onePercentError(int)
         */
        public static IBuilder<ICardinality> obyCount(long maxCardinality)
        {
            if(maxCardinality <= 0) throw new IllegalArgumentException("maxCardinality ("+maxCardinality+") must be a positive integer");

            if(maxCardinality < 4250000)
            {
                return LinearCounting.Builder.onePercentError((int)maxCardinality);
            }

            return new Builder(16);
        }
    }
}


//----------------------------------------------------
//copy from Stream-Lib, only and have to add Serializable
class LogLog implements ICardinality, Serializable
{
	private static final long serialVersionUID = 1L;
    public LogLog() { this(1); }

    /**
     * Gamma function computed using SciLab
     * ((gamma(-(m.^(-1))).* ( (1-2.^(m.^(-1)))./log(2) )).^(-m)).*m
     */
    protected static final double[] mAlpha = {
            0,
            0.44567926005415,
            1.2480639342271,
            2.8391255240079,
            6.0165231584811,
            12.369319965552,
            25.073991603109,
            50.482891762521,
            101.30047482549,
            202.93553337953,
            406.20559693552,
            812.74569741657,
            1625.8258887309,
            3251.9862249084,
            6504.3071471860,
            13008.949929672,
            26018.222470181,
            52036.684135280,
            104073.41696276,
            208139.24771523,
            416265.57100022,
            832478.53851627,
            1669443.2499579,
            3356902.8702907,
            6863377.8429508,
            11978069.823687,
            31333767.455026,
            52114301.457757,
            72080129.928986,
            68945006.880409,
            31538957.552704,
            3299942.4347441
    };

    protected final int k;
    protected int m;
    protected double Ca;
    protected byte[] M;
    protected int Rsum = 0;

    public LogLog(int k)
    {
        if(k >= mAlpha.length) throw new IllegalArgumentException(String.format("Max k (%d) exceeded: k=%d", mAlpha.length-1, k));

        this.k = k;
        this.m = 1 << k;
        this.Ca = mAlpha[k];
        this.M = new byte[m];
    }

    public LogLog(byte[] M)
    {
        this.M = M;
        this.m =  M.length;
        this.k = Integer.numberOfTrailingZeros(m);
        assert(m == (1 << k)) : "Invalid array size: M.length must be a power of 2";
        this.Ca = mAlpha[k];
        for(byte b : M)
        {
            Rsum += b;
        }
    }

    @Override
    public byte[] getBytes()
    {
        return M;
    }

    public int sizeof()
    {
        return m;
    }

    @Override
    public long cardinality()
    {
        /*
        for(int j=0; j<m; j++)
            System.out.print(M[j]+"|");
        System.out.println();
        */

        double Ravg = Rsum / (double)m;
        return (long)(Ca * java.lang.Math.pow(2, Ravg));
    }

    @Override
    public boolean offer(Object o)
    {
        boolean modified = false;

        int x = MurmurHash.hash(o);
        int j = x >>> (Integer.SIZE - k);
        byte r = (byte)(Integer.numberOfLeadingZeros( (x << k) | (1<<(k-1)) )+1);
        if(M[j] < r)
        {
            Rsum += r-M[j];
            M[j] = r;
            modified = true;
        }

        return modified;
    }

    @Override
    public boolean offerHashed(long hashedLong) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offerHashed(int hashedInt) {
        boolean modified = false;
        int j = hashedInt >>> (Integer.SIZE - k);
        byte r = (byte) (Integer.numberOfLeadingZeros((hashedInt << k) | (1 << (k - 1))) + 1);
        if (M[j] < r) {
            Rsum += r - M[j];
            M[j] = r;
            modified = true;
        }

        return modified;
    }

    /**
     * Computes the position of the first set bit of the last Integer.SIZE-k bits
     *
     * @return Integer.SIZE-k if the last k bits are all zero
     */
    protected static int rho(int x, int k)
    {
        return Integer.numberOfLeadingZeros( (x << k) | (1<<(k-1)) )+1;
    }

    /**
     * @return this if estimators is null or no arguments are passed
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be instances of LogLog of the same size)
     */
    @Override
    public ICardinality merge(ICardinality... estimators) throws LogLogMergeException
    {
        if(estimators == null || estimators.length == 0)
        {
            return this;
        }
        byte[] mergedBytes = Arrays.copyOf(this.M, this.M.length);

        for(ICardinality estimator : estimators)
        {
            if(!(this.getClass().isInstance(estimator)))
            {
                throw new LogLogMergeException("Cannot merge estimators of different class");
            }
            if(estimator.sizeof() != this.sizeof())
            {
                throw new LogLogMergeException("Cannot merge estimators of different sizes");
            }
            LogLog ll = (LogLog) estimator;
            for(int i = 0; i < mergedBytes.length; ++i)
            {
                mergedBytes[i] = (byte)java.lang.Math.max(mergedBytes[i], ll.M[i]);
            }
        }

        return new LogLog(mergedBytes);
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be the same size)
     */
    public static LogLog mergeEstimators(LogLog... estimators) throws LogLogMergeException
    {
        if(estimators == null || estimators.length == 0)
        {
            return null;
        }
        return (LogLog)estimators[0].merge(Arrays.copyOfRange(estimators, 1, estimators.length));
    }


    @SuppressWarnings("serial")
    protected static class LogLogMergeException extends CardinalityMergeException
    {
        public LogLogMergeException(String message)
        {
            super(message);
        }
    }

    public static class Builder implements IBuilder<ICardinality>
    {
        protected final int k;

        public Builder()
        {
            this(16);
        }

        public Builder(int k)
        {
            this.k = k;
        }

        @Override
        public LogLog build()
        {
            return new LogLog(k);
        }

        @Override
        public int sizeof()
        {
            return 1 << k;
        }
    }
}
