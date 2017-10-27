package com.pplive.pike.function.builtin;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.util.IBuilder;
import com.clearspring.analytics.hash.MurmurHash;

import java.io.*;
import java.lang.Math;
import java.util.Arrays;

/**
 * Created by jiatingjin on 2017/10/23.
 */
// SerializableHyperLoglogCounting is not Serializable, so we have to create a wrapper
class SerializableHyperLoglogCounting extends HyperLogLog implements Serializable {

    private static final long serialVersionUID = 1L;

    public SerializableHyperLoglogCounting() {
        super(16);
    }

    public SerializableHyperLoglogCounting(HyperLogLog other) {
        super(other);
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

public final class HyperLoglogCountState implements Serializable {
    private static final long serialVersionUID = 1L;

    public boolean isOneData() { return _counting == null; }
    private final Object[] _objs;

    private final SerializableHyperLoglogCounting _counting;

    public HyperLoglogCountState(SerializableHyperLoglogCounting counting) {
        assert counting != null;
        this._counting = counting;
        this._objs = null;
    }

    public HyperLoglogCountState(Object[] objs) {
        this._counting = null;
        assert objs != null;
        this._objs = objs;
    }

    public static HyperLoglogCountState combineNonNull(HyperLoglogCountState left, HyperLoglogCountState right) {
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
                HyperLogLog c = HyperLogLog.mergeEstimators(left._counting, right._counting);
                return new HyperLoglogCountState(new SerializableHyperLoglogCounting(c));
            }
            catch(CardinalityMergeException e) {
                throw new IllegalStateException(e);
            }
        }
        else {
            assert false;
            throw new IllegalStateException("should never happen: HyperLoglogCountState combine(): left and right both are one data");
        }
    }

    public long cardinality() { return isOneData() ? 1L : this._counting.cardinality();}
}


//----------------------------------------------------
//copy from Stream-Lib, only and have to add Serializable
class HyperLogLog implements ICardinality, Serializable {

    private final RegisterSet registerSet;
    private final int log2m;
    private final double alphaMM;


    /**
     * Create a new HyperLogLog instance using the specified standard deviation.
     *
     * @param rsd - the relative standard deviation for the counter.
     *            smaller values create counters that require more space.
     */
    public HyperLogLog(double rsd) {
        this(log2m(rsd));
    }

    public HyperLogLog(HyperLogLog hll) {
        this(hll.log2m, hll.registerSet);
    }


    private static int log2m(double rsd) {
        return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

    private static double rsd(int log2m) {
        return 1.106 / Math.sqrt(Math.exp(log2m * Math.log(2)));
    }

    private static void validateLog2m(int log2m) {
        if (log2m < 0 || log2m > 30) {
            throw new IllegalArgumentException("log2m argument is "
                    + log2m + " and is outside the range [0, 30]");
        }
    }

    /**
     * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
     * the counter.  The larger the log2m the better the accuracy.
     * <p/>
     * accuracy = 1.04/sqrt(2^log2m)
     *
     * @param log2m - the number of bits to use as the basis for the HLL instance
     */
    public HyperLogLog(int log2m) {
        this(log2m, new RegisterSet(1 << log2m));
    }

    /**
     * Creates a new HyperLogLog instance using the given registers.  Used for unmarshalling a serialized
     * instance and for merging multiple counters together.
     *
     * @param registerSet - the initial values for the register set
     */
    @Deprecated
    public HyperLogLog(int log2m, RegisterSet registerSet) {
        validateLog2m(log2m);
        this.registerSet = registerSet;
        this.log2m = log2m;
        int m = 1 << this.log2m;

        alphaMM = getAlphaMM(log2m, m);
    }


    public boolean offerHashed(long hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = (int) (hashedValue >>> (Long.SIZE - log2m));
        final int r = Long.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }


    public boolean offerHashed(int hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = hashedValue >>> (Integer.SIZE - log2m);
        final int r = Integer.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }

    @Override
    public boolean offer(Object o) {
        final int x = MurmurHash.hash(o);
        return offerHashed(x);
    }


    @Override
    public long cardinality() {
        double registerSum = 0;
        int count = registerSet.count;
        double zeros = 0.0;
        for (int j = 0; j < registerSet.count; j++) {
            int val = registerSet.get(j);
            registerSum += 1.0 / (1 << val);
            if (val == 0) {
                zeros++;
            }
        }

        double estimate = alphaMM * (1 / registerSum);

        if (estimate <= (5.0 / 2.0) * count) {
            // Small Range Estimate
            return Math.round(linearCounting(count, zeros));
        } else {
            return Math.round(estimate);
        }
    }

    @Override
    public int sizeof() {
        return registerSet.size * 4;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        writeBytes(dos);

        return baos.toByteArray();
    }

    private void writeBytes(DataOutput serializedByteStream) throws IOException {
        serializedByteStream.writeInt(log2m);
        serializedByteStream.writeInt(registerSet.size * 4);
        for (int x : registerSet.readOnlyBits()) {
            serializedByteStream.writeInt(x);
        }
    }

    /**
     * Add all the elements of the other set to this set.
     * <p/>
     * This operation does not imply a loss of precision.
     *
     * @param other A compatible Hyperloglog instance (same log2m)
     * @throws CardinalityMergeException if other is not compatible
     */
    public void addAll(HyperLogLog other) throws CardinalityMergeException {
        if (this.sizeof() != other.sizeof()) {
            throw new HyperLogLogMergeException("Cannot merge estimators of different sizes");
        }

        registerSet.merge(other.registerSet);
    }

    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        HyperLogLog merged = new HyperLogLog(log2m, new RegisterSet(this.registerSet.count));
        merged.addAll(this);

        if (estimators == null) {
            return merged;
        }

        for (ICardinality estimator : estimators) {
            if (!(estimator instanceof HyperLogLog)) {
                throw new HyperLogLogMergeException("Cannot merge estimators of different class");
            }
            HyperLogLog hll = (HyperLogLog) estimator;
            merged.addAll(hll);
        }

        return merged;
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws CardinalityMergeException if estimators are not mergeable (all estimators must be the same size)
     */
    public static HyperLogLog mergeEstimators(HyperLogLog... estimators) throws CardinalityMergeException
    {
        if(estimators == null || estimators.length == 0)
        {
            return null;
        }
        return (HyperLogLog)estimators[0].merge(Arrays.copyOfRange(estimators, 1, estimators.length));
    }

    private Object writeReplace() {
        return new SerializationHolder(this);
    }

    /**
     * This class exists to support Externalizable semantics for
     * HyperLogLog objects without having to expose a public
     * constructor, public write/read methods, or pretend final
     * fields aren't final.
     *
     * In short, Externalizable allows you to skip some of the more
     * verbose meta-data default Serializable gets you, but still
     * includes the class name. In that sense, there is some cost
     * to this holder object because it has a longer class name. I
     * imagine people who care about optimizing for that have their
     * own work-around for long class names in general, or just use
     * a custom serialization framework. Therefore we make no attempt
     * to optimize that here (eg. by raising this from an inner class
     * and giving it an unhelpful name).
     */
    private static class SerializationHolder implements Externalizable {

        HyperLogLog hyperLogLogHolder;

        public SerializationHolder(HyperLogLog hyperLogLogHolder) {
            this.hyperLogLogHolder = hyperLogLogHolder;
        }

        /**
         * required for Externalizable
         */
        public SerializationHolder() {

        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            hyperLogLogHolder.writeBytes(out);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            hyperLogLogHolder = Builder.build(in);
        }

        private Object readResolve() {
            return hyperLogLogHolder;
        }
    }

    public static class Builder implements IBuilder<ICardinality>, Serializable {
        private static final long serialVersionUID = -2567898469253021883L;

        private final double rsd;
        private transient int log2m;

        /**
         * Uses the given RSD percentage to determine how many bytes the constructed HyperLogLog will use.
         *
         * @deprecated Use {@link #withRsd(double)} instead. This builder's constructors did not match the (already
         * themselves ambiguous) constructors of the HyperLogLog class, but there is no way to make them match without
         * risking behavior changes downstream.
         */
        @Deprecated
        public Builder(double rsd) {
            this.log2m = log2m(rsd);
            validateLog2m(log2m);
            this.rsd = rsd;
        }

        /** This constructor is private to prevent behavior change for ambiguous usages. (Legacy support). */
        private Builder(int log2m) {
            this.log2m = log2m;
            validateLog2m(log2m);
            this.rsd = rsd(log2m);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            this.log2m = log2m(rsd);
        }

        @Override
        public HyperLogLog build() {
            return new HyperLogLog(log2m);
        }

        @Override
        public int sizeof() {
            int k = 1 << log2m;
            return RegisterSet.getBits(k) * 4;
        }

        public static Builder withLog2m(int log2m) {
            return new Builder(log2m);
        }

        public static Builder withRsd(double rsd) {
            return new Builder(rsd);
        }

        public static HyperLogLog build(byte[] bytes) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            return build(new DataInputStream(bais));
        }

        public static HyperLogLog build(DataInput serializedByteStream) throws IOException {
            int log2m = serializedByteStream.readInt();
            int byteArraySize = serializedByteStream.readInt();
            return new HyperLogLog(log2m,
                    new RegisterSet(1 << log2m, getBits(serializedByteStream, byteArraySize)));
        }

        public static int[] getBits(byte[] mBytes) throws IOException {
            int bitSize = mBytes.length / 4;
            int[] bits = new int[bitSize];
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(mBytes));
            for (int i = 0; i < bitSize; i++) {
                bits[i] = dis.readInt();
            }
            return bits;
        }

        /**
         * This method might be better described as
         * "byte array to int array" or "data input to int array"
         */
        public static int[] getBits(DataInput dataIn, int byteLength) throws IOException {
            int bitSize = byteLength / 4;
            int[] bits = new int[bitSize];
            for (int i = 0; i < bitSize; i++) {
                bits[i] = dataIn.readInt();
            }
            return bits;
        }
    }

    @SuppressWarnings("serial")
    protected static class HyperLogLogMergeException extends CardinalityMergeException {

        public HyperLogLogMergeException(String message) {
            super(message);
        }
    }

    protected static double getAlphaMM(final int p, final int m) {
        // See the paper.
        switch (p) {
            case 4:
                return 0.673 * m * m;
            case 5:
                return 0.697 * m * m;
            case 6:
                return 0.709 * m * m;
            default:
                return (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }

    protected static double linearCounting(int m, double V) {
        return m * Math.log(m / V);
    }
}


class RegisterSet {

    public final static int LOG2_BITS_PER_WORD = 6;
    public final static int REGISTER_SIZE = 5;

    public final int count;
    public final int size;

    private final int[] M;

    public RegisterSet(int count) {
        this(count, null);
    }

    public RegisterSet(int count, int[] initialValues) {
        this.count = count;

        if (initialValues == null) {
            this.M = new int[getSizeForCount(count)];
        } else {
            this.M = initialValues;
        }
        this.size = this.M.length;
    }

    public static int getBits(int count) {
        return count / LOG2_BITS_PER_WORD;
    }

    public static int getSizeForCount(int count) {
        int bits = getBits(count);
        if (bits == 0) {
            return 1;
        } else if (bits % Integer.SIZE == 0) {  //why ???, I think count % LOG2_BITS_PER_WORD is correct
            return bits;
        } else {
            return bits + 1;
        }
    }

    public void set(int position, int value) {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        this.M[bucketPos] = (this.M[bucketPos] & ~(0x1f << shift)) | (value << shift);
    }

    public int get(int position) {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (this.M[bucketPos] & (0x1f << shift)) >>> shift;
    }

    public boolean updateIfGreater(int position, int value) {
        int bucket = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
        int mask = 0x1f << shift;

        // Use long to avoid sign issues with the left-most shift
        long curVal = this.M[bucket] & mask;
        long newVal = value << shift;
        if (curVal < newVal) {
            this.M[bucket] = (int) ((this.M[bucket] & ~mask) | newVal);
            return true;
        } else {
            return false;
        }
    }

    public void merge(RegisterSet that) {
        for (int bucket = 0; bucket < M.length; bucket++) {
            int word = 0;
            for (int j = 0; j < LOG2_BITS_PER_WORD; j++) {
                int mask = 0x1f << (REGISTER_SIZE * j);

                int thisVal = (this.M[bucket] & mask);
                int thatVal = (that.M[bucket] & mask);
                word |= (thisVal < thatVal) ? thatVal : thisVal;
            }
            this.M[bucket] = word;
        }
    }

    int[] readOnlyBits() {
        return M;
    }

    public int[] bits() {
        int[] copy = new int[size];
        System.arraycopy(M, 0, copy, 0, M.length);
        return copy;
    }
}