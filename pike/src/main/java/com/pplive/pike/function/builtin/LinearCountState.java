package com.pplive.pike.function.builtin;

import java.io.Serializable;
import java.util.Arrays;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.util.IBuilder;

// LinearCounting is not Serializable, so we have to create a wrapper
class SerializableLinearCounting extends LinearCounting implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public SerializableLinearCounting() {
		super();
	}
	
	public SerializableLinearCounting(LinearCounting other) {
		super(other);
	}

	// logic is copied from parent class, just change hash procedure.
    public boolean offer(Object[] objs)
    {
    	if (objs.length == 1) {
    		return offer(objs[0]);
    	}
    	
        boolean modified = false;

        long hash = hash(objs);
        
        int bit = (int)((hash & 0xFFFFFFFFL) % (long)length);
        int i = bit/8;

        if (_longByteArray != null) {
            byte b = _longByteArray.getByte(i);
            byte mask = (byte)(1 << (bit % 8));
            if((mask & b) == 0)
            {
                _longByteArray.setByte(i, (byte) (b | mask));
                count--;
                modified = true;
            }
        }
        else {
            byte b = map[i];
            byte mask = (byte)(1 << (bit % 8));
            if((mask & b) == 0)
            {
                map[i] = (byte) (b | mask);
                count--;
                modified = true;
            }
        }

        return modified;
    }
    
    private static long hash(Object[] objs){
    	assert objs != null && objs.length > 0;
    	
        long hash = (long)MurmurHash.hash(objs[0]);
        for(int n = 1; n < objs.length; n += 1){
        	hash ^= MurmurHash.hash(objs[n]);
        }
        return hash;
    }
}

final class LinearCountState implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public boolean isOneData() { return _counting == null; }
	private final Object[] _objs;

	private final SerializableLinearCounting _counting;
	
	public LinearCountState(SerializableLinearCounting counting) {
		assert counting != null;
		this._counting = counting;
		this._objs = null;
	}

	public LinearCountState(Object[] objs) {
		this._counting = null;
		assert objs != null;
		this._objs = objs;
	}
	
	public long cardinality() {
		return isOneData() ? 1L : this._counting.cardinality();
	}
	
	public static LinearCountState combineNonNull(LinearCountState left, LinearCountState right) {
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
				LinearCounting c = LinearCounting.mergeEstimators(left._counting, right._counting);
				return new LinearCountState(new SerializableLinearCounting(c));
			}
			catch(CardinalityMergeException e) {
				throw new IllegalStateException(e);
			}
		}
		else {
			assert false;
			throw new IllegalStateException("should never happen: LinearCountState combine(): left and right both are one data");
		}
	}

    @Override
    public String toString() {
        String s = String.format("_objs: %s, _counting: %s", _objs, _counting);
        return s;
    }
}


// ----------------------------------------------------
// copy from Stream-Lib, only and have to add Serializable
/**
 * See <i>A Linear-Time Probabilistic Counting Algorithm for Database Applications</i>
 * by Whang, Vander-Zanden, Taylor
 *
 */
class LinearCounting implements ICardinality, Serializable
{
	private static final long serialVersionUID = 1L;
    public LinearCounting() { this(1); }

	/**
     * Bitmap
     * Hashed stream elements are mapped to bits in this array
     */
    protected byte[] map;

    protected transient LongUnitByteArray _longByteArray;

    /**
     * Size of the map in bits
     */
    protected final int length;


    /**
     * Number of bits left unset in the map
     */
    protected int count;

    /**
     *
     * @param mapBytes of bit array in bytes
     */
    public LinearCounting(int mapBytes)
    {
        this(mapBytes, true);
    }

    public LinearCounting(int mapBytes, boolean mapUseLongByteArray)
    {
        if (mapUseLongByteArray) {
            mapBytes = ((mapBytes + 63) >> 6) << 6;
            this._longByteArray = new LongUnitByteArray(mapBytes);
        }
        else {
            map = new byte[mapBytes];
        }
        this.length = 8 * mapBytes;
        this.count = this.length;
    }

    public LinearCounting(byte[] map)
    {
        this.map = map;
        this.length = 8 * map.length;
        this.count = computeCount();
    }

    public LinearCounting(LongUnitByteArray longByteArray)
    {
        this._longByteArray = longByteArray;
        this.length = 8 * longByteArray.byteSize();
        this.count = computeCount();
    }

    public LinearCounting(LinearCounting other) {
        this._longByteArray = other._longByteArray;
        this.map = other.map;
        this.length = other.length;
        this.count = other.count;
     }

    @Override
    public String toString() {
        String s = String.format("longByteArray: %s, map: %s, length: %d, count: %d", _longByteArray, map, length, count);
        return s;
    }

    @Override
    public long cardinality()
    {
        return (long)(java.lang.Math.round(length*java.lang.Math.log(length/((double)count))));
    }

    @Override
    public byte[] getBytes()
    {
        return map;
    }

    @Override
    public boolean offer(Object o)
    {
        boolean modified = false;

        long hash = (long)MurmurHash.hash(o);
        int bit = (int)((hash & 0xFFFFFFFFL) % (long)length);
        int i = bit/8;

        if (_longByteArray != null) {
            byte b = _longByteArray.getByte(i);
            byte mask = (byte)(1 << (bit % 8));
            if((mask & b) == 0)
            {
                _longByteArray.setByte(i, (byte) (b | mask));
                count--;
                modified = true;
            }
        }
        else {
            byte b = map[i];
            byte mask = (byte)(1 << (bit % 8));
            if((mask & b) == 0)
            {
                map[i] = (byte) (b | mask);
                count--;
                modified = true;
            }
        }

        return modified;
    }

    @Override
    public boolean offerHashed(long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offerHashed(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int sizeof()
    {
        return _longByteArray != null ? _longByteArray.byteSize() : map.length;
    }

    protected int computeCount()
    {
        int c=0;
        if (this._longByteArray != null) {
            for(int n = 0; n < _longByteArray.longSize(); n += 1) {
               long l = _longByteArray.getLong(n);
               c += Long.bitCount(l);
            }
        }
        else {
            for(byte b : map)
            {
                c+= Integer.bitCount(b & 0xFF);
            }
        }

        return length - c;
    }

    /**
     *
     * @return (# set bits) / (total # of bits)
     */
    public double getUtilization()
    {
        return (length-count) / (double) length;
    }

    public int getCount()
    {
        return count;
    }

    public boolean isSaturated()
    {
        return (count == 0);
    }

    /**
     * For debug purposes
     * @return
     */
    protected String mapAsBitString()
    {
        StringBuilder sb = new StringBuilder();
        if (this._longByteArray != null) {
            for(int n = 0; n < _longByteArray.longSize(); n += 1) {
                long l = _longByteArray.getLong(n);
                String bits = Long.toBinaryString(l);
                for(int i = 0; i < 64 - bits.length(); i++) sb.append('0');
                sb.append(bits);
            }

        }
        else {
            for(byte b : map)
            {
                String bits = Integer.toBinaryString(b);
                for(int i = 0; i < 8 - bits.length(); i++) sb.append('0');
                sb.append(bits);
            }
        }
        return sb.toString();
    }

    /**
     * @return this if estimators is null or no arguments are passed
     * @throws LinearCountingMergeException if estimators are not mergeable (all estimators must be instances of LinearCounting of the same size)
     */
    @Override
    public ICardinality merge(ICardinality... estimators) throws LinearCountingMergeException
    {
        if(estimators == null || estimators.length == 0)
        {
            return this;
        }
        LinearCounting[] lcs = Arrays.copyOf(estimators, estimators.length + 1, LinearCounting[].class);
        lcs[lcs.length - 1] = this;
        return LinearCounting.mergeEstimators(lcs);
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws LinearCountingMergeException if estimators are not mergeable (all estimators must be the same size)
     */
    public static LinearCounting mergeEstimators(LinearCounting... estimators) throws LinearCountingMergeException
    {
        LinearCounting merged = null;

        if (estimators != null && estimators.length > 0 && estimators[0]._longByteArray != null) {
            int bytes = estimators[0]._longByteArray.byteSize();
            LongUnitByteArray mergedBytes = new LongUnitByteArray(bytes);

            for (LinearCounting estimator : estimators)
            {
                if (estimator._longByteArray.byteSize() != bytes)
                {
                    throw new LinearCountingMergeException("Cannot merge estimators of different sizes");
                }

                LongUnitByteArray right = estimator._longByteArray;
                for (int n = 0; n < mergedBytes.longSize(); n++)
                {
                    mergedBytes.setLong(n, mergedBytes.getLong(n) | right.getLong(n));
                }
            }

            merged = new LinearCounting(mergedBytes);
            return merged;
        }

        if(estimators != null && estimators.length > 0)
        {
            int size = estimators[0].map.length;
            byte[] mergedBytes = new byte[size];

            for (LinearCounting estimator : estimators)
            {
                if (estimator.map.length != size)
                {
                    throw new LinearCountingMergeException("Cannot merge estimators of different sizes");
                }

                for (int b = 0; b < size; b++)
                {
                    mergedBytes[b] |= estimator.map[b];
                }
            }

            merged = new LinearCounting(mergedBytes);
        }
        return merged;
    }

    @SuppressWarnings("serial")
    protected static class LinearCountingMergeException extends CardinalityMergeException
    {
        public LinearCountingMergeException(String message)
        {
            super(message);
        }
    }

    public static class Builder implements IBuilder<ICardinality>, Serializable
    {
        private static final long serialVersionUID = -4245416224034648428L;

        /**
         * Taken from Table II of Whang et al.
         */
        protected final static int[] onePercentErrorLength =
        {
            5034, 5067, 5100, 5133, 5166, 5199, 5231, 5264, 5296,                    // 100 - 900
            5329, 5647, 5957, 6260, 6556, 6847, 7132, 7412, 7688,                    // 1000 - 9000
            7960, 10506, 12839, 15036, 17134, 19156, 21117, 23029, 24897,            // 10000 - 90000
            26729, 43710, 59264, 73999, 88175, 101932, 115359, 128514, 141441,       // 100000 - 900000
            154171, 274328, 386798, 494794, 599692, 702246, 802931, 902069, 999894,  // 1000000 - 9000000
            1096582                                                                  // 10000000
        };

        protected final int size;

        public Builder()
        {
            this(65536);
        }

        public Builder(int size)
        {
            this.size = size;
        }

        @Override
        public LinearCounting build()
        {
            return new LinearCounting(size, true);
        }

        @Override
        public int sizeof()
        {
            return size;
        }

        /**
         * Returns a LinearCounting.Builder that generates an LC
         * estimator which keeps estimates below 1% error on average and has
         * a low likelihood of saturation (0.7%) for any stream with
         * cardinality less than maxCardinality
         *
         * @param maxCardinality
         * @throws IllegalArgumentException if maxCardinality is not a positive integer
         * @return
         */
        public static Builder onePercentError(int maxCardinality)
        {
            if(maxCardinality <= 0) throw new IllegalArgumentException("maxCardinality ("+maxCardinality+") must be a positive integer");

            int length = -1;
            if(maxCardinality < 100)
            {
                length = onePercentErrorLength[0];
            }
            else if(maxCardinality < 10000000)
            {
                int logscale = (int)java.lang.Math.log10(maxCardinality);
                int scaleValue = (int)java.lang.Math.pow(10, logscale);
                int scaleIndex = maxCardinality / scaleValue;
                int index = 9*(logscale-2)+(scaleIndex-1);
                int lowerBound = scaleValue*scaleIndex;
                length = lerp(lowerBound, onePercentErrorLength[index], lowerBound+scaleValue, onePercentErrorLength[index+1], maxCardinality);

                //System.out.println(String.format("Lower bound: %9d, Max cardinality: %9d, Upper bound: %9d", lowerBound, maxCardinality, lowerBound+scaleValue));
                //System.out.println(String.format("Lower bound: %9d, Interpolated   : %9d, Upper bound: %9d", onePercentErrorLength[index], length, onePercentErrorLength[index+1]));
            }
            else if(maxCardinality < 50000000)
            {
                length = lerp(10000000, 1096582, 50000000, 4584297, maxCardinality);
            }
            else if(maxCardinality < 100000000)
            {
                length = lerp(50000000, 4584297, 100000000, 8571013, maxCardinality);
            }
            else if(maxCardinality <= 120000000)
            {
                length = lerp(100000000, 8571013, 120000000, 10112529, maxCardinality);
            }
            else
            {
                length = maxCardinality / 12;
            }

            int sz = (int)java.lang.Math.ceil(length / 8D);

            //System.out.println("length: "+length+", size (bytes): "+sz);

            return new Builder(sz);
        }

        /**
         *
         * @param x0
         * @param y0
         * @param x1
         * @param y1
         * @param x
         * @return linear interpolation
         */
        protected static int lerp(int x0, int y0, int x1, int y1, int x)
        {
            return (int)java.lang.Math.ceil(y0+(x-x0)*(double)(y1-y0)/(x1-x0));
        }
    }
}

final class LongUnitByteArray {

    private long[] _longBytes;
    public long[] longArray() {
        return _longBytes;
    }

    public LongUnitByteArray(int bytes) {
        if (bytes < 0 || (bytes % 8) != 0) {
            throw new IllegalArgumentException("bytes must be > 0 and value of integer N * 8");
        }

        _longBytes = new long[bytes >> 3];
    }

    public int byteSize() {
        return _longBytes.length * 8;
    }

    public int longSize() {
        return _longBytes.length;
    }

    public long getLong(int index) {
        return _longBytes[index];
    }

    public void setLong(int index, long val) {
        _longBytes[index] = val;
    }

    public byte getByte(int index) {
        int longIndex = index >> 3;
        int indexInLong = index & 0x07;
        long n = _longBytes[longIndex];
        n >>= 8 * indexInLong;
        return (byte)(n & 0xff);
    }

    public void setByte(int index, byte val) {
        int longIndex = index >> 3;
        int indexInLong = index & 0x07;
        long n = _longBytes[longIndex];
        switch(indexInLong) {
            case 0: n &= 0xffffffffffffff00L; break;
            case 1: n &= 0xffffffffffff00ffL; break;
            case 2: n &= 0xffffffffff00ffffL; break;
            case 3: n &= 0xffffffff00ffffffL; break;
            case 4: n &= 0xffffff00ffffffffL; break;
            case 5: n &= 0xffff00ffffffffffL; break;
            case 6: n &= 0xff00ffffffffffffL; break;
            case 7: n &= 0x00ffffffffffffffL; break;
        }

        long m = (val & 0xffL);
        m <<= 8 * indexInLong;
        _longBytes[longIndex] = (n | m);
    }
}