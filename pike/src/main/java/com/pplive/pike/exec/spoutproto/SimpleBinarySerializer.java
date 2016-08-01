package com.pplive.pike.exec.spoutproto;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

// this class use a private binary format to serialize/deserialize base type data.
// the principle of format definition is make most appeared data using minimal size,
// so we use a variable length integer/float number format.
// define:
// boolean: use one byte, true is 1, false is 0
// integer:
//   first byte: MORE: 0 means end, 1 means has more following byte.
//               SIGN: 0 means positive, 1 means negative
//   ------------------------------------
//   |MORE|SIGN|    VALUE(6bits)        |
//   ------------------------------------
//   following bytes: MORE: same as in first byte
//   ------------------------------------
//   |MORE|       VALUE(7bits)          |
//   ------------------------------------
//   the number is concat all value bits, convert to a positive number, then apply the SIGN
//
// float:
//   ---------------------------------------------------
//   |Integer(1-2bytes)|       Integer(1-8bytes)       |
//   ---------------------------------------------------
//   the first integer is exponent part in IEEE floating 64-bits format
//   the second integer is significand part in IEEE floating 64-bits format, plus SIGN
//
// string:
//   ---------------------------------------------------
//   |Length(1+bytes)|      VALUE(<Length>bytes)       |
//   ---------------------------------------------------
//   Length is an integer, followed by <Length> bytes of the string utf-8 encoding result.
//
class SimpleBinarySerializer {

    //------------------------------------------------------------------
    // methods for serialize

    private GrowableByteBuffer _buffer;
    public GrowableByteBuffer buffer() {
        return this._buffer;
    }

    public void beginSerialize(GrowableByteBuffer buffer){
        assert buffer != null;

        this._buffer = buffer;
    }

    public GrowableByteBuffer endSerialize(){
        GrowableByteBuffer buffer = this._buffer;
        this._buffer = null;
        return buffer;
    }

    private void ensureBuffer(int requiredExtraBytes){
        assert requiredExtraBytes > 0;
        this._buffer.ensureBufferRemaning(requiredExtraBytes);
    }

    public void addBoolean(boolean val){
        addByte(val ? (byte) 1 : (byte) 0);
    }

    public void addByte(byte val){
        ensureBuffer(1);
        putByte(val);
    }

    private void putByte(byte val){
        this._buffer.buffer().put(val);
    }

    public void addInt16(short val){
        addInt64(val);
    }

    public void addInt32(int val){
        addInt64(val);
    }

    public void addInt64(long val){
        ensureBuffer(10);

        boolean negative = val < 0;
        val = Math.abs(val);
        byte b = (byte)(val & 0x3f);
        val >>>= 6;  // MUST be >>> for handle Long.MIN_VALUE correctly
        if (val > 0){
            b |= 0x80;
        }
        if (negative){
            b |= 0x40;
        }
        putByte(b);

        while(val > 0){
            b = (byte)(val & 0x7f);
            val >>>= 7;
            if (val > 0){
               b |= 0x80;
            }
            putByte(b);
        }
    }

    public void addFloat(float val){
        addDouble(val);

    }

    public void addDouble(double val){
        long bits = Double.doubleToRawLongBits(val);
        int exponent = (int)((bits & 0x7ff0000000000000L) >>> 52);
        exponent -= 1023; // 3FF
        long significand = (bits & 0x000fffffffffffffL);
        significand += 1; // avoid significand == 0 causing negative sign lost
        if ((bits & 0x8000000000000000L) != 0){
            significand = -significand;
        }
        addInt64(significand);
        addInt32(exponent);
    }

    private final static Charset _utf8Charset = Charset.forName("utf-8");

    public void addString(String val){
        if(val.length() == 0){
            addInt32(-63);
            return;
        }

        byte[] utf8Bytes = val.getBytes(_utf8Charset);
        ensureBuffer(10 + utf8Bytes.length);
        addInt32(utf8Bytes.length - 63);   // optimize, since length always >= 0,
                                           // so this make larger length (up to 127) can be put in only one byte
        this._buffer.buffer().put(utf8Bytes);
    }

    public void addBytes(byte[] val){
        addBytes(val, 0, val.length);
    }

    public void addBytes(byte[] val, int offset, int length){
        assert val != null;
        assert offset >= 0 && length >= 0;
        assert offset + length <= val.length;

        if(length == 0){
            addInt32(-63);
            return;
        }

        addInt32(length - 63); // see comments in addString()
        ensureBuffer(val.length);
        this._buffer.buffer().put(val, offset, length);
    }

    //------------------------------------------------------------------
    // methods for deserialize

    public void beginDeserialize(GrowableByteBuffer buffer){
        assert buffer != null;
        this._buffer = buffer;
    }

    public void endDeserialize(){
        this._buffer = null;
    }

    public int getRemainingBytes(){
        return this._buffer.remaining();
    }

    public boolean nextBoolean(){
        byte b = nextByte();
        return b != 0;
    }

    public byte nextByte(){
        byte b = this._buffer.buffer().get();
        return b;
    }

    public short nextInt16(){
        long n = nextInt64();
        return (short)n;
    }

    public int nextInt32(){
        long n = nextInt64();
        return (int)n;
    }

    public long nextInt64(){
        byte b = nextByte();
        boolean negative = (b & 0x40) != 0;
        boolean more = (b & 0x80) != 0;
        b &= 0x3f;
        long n = b;
        int bits = 6;
        while(more){
            b = nextByte();
            more = (b & 0x80) != 0;
            b &= 0x7f;
            n = (((long)b)<<bits) | n;
            bits += 7;
        }

        if (negative){
            n = -n;
        }
        return n;
    }

    public float nextFloat(){
        double d = nextDouble();
        return (float)d;
    }

    public double nextDouble(){
        long significand = nextInt64();
        int exponent = nextInt32();
        exponent += 1023; // 3FF
        boolean negative = significand < 0;
        if (negative) {
            significand = -significand;
        }
        significand -= 1;  // see comments in addDouble()
        long bits = exponent;
        bits <<= 52;
        bits |= significand;
        if (negative){
            bits |= 0x8000000000000000L;
        }
        return Double.longBitsToDouble(bits);
    }

    public String nextString(){
        int len = nextInt32() + 63;  // see comments in addString()
        if (len == 0){
            return "";
        }
        if (len < 0 || len > getRemainingBytes()){
            throw new BinaryDeserializeException("length error in deserializing string");
        }

        int limit = this._buffer.limit();
        this._buffer.limit(this._buffer.position() + len);
        CharBuffer chars;
        try {
            chars =_utf8Charset.newDecoder().decode(this._buffer.buffer());
        }
        catch(CharacterCodingException e){
            throw new BinaryDeserializeException(e);
        }
        finally{
            assert limit >= this._buffer.limit();
            this._buffer.limit(limit);
        }
        return chars.toString();
    }

    public byte[] nextBytes() {
        int len = nextInt32() + 63;  // see comments in addString()
        if (len == 0){
            return new byte[0];
        }
        if (len < 0 || len > getRemainingBytes()){
            throw new BinaryDeserializeException("length error in deserializing byte[]");
        }

        byte[] bytes = new byte[len];
        this._buffer.buffer().get(bytes);
        return bytes;
    }
}
