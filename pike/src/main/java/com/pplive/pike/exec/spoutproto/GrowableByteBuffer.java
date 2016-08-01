package com.pplive.pike.exec.spoutproto;

import java.nio.ByteBuffer;
import java.util.Arrays;

// this class wrap the ByteBuffer class, add growing buffer capability
class GrowableByteBuffer {

    private byte[] _data;
    public byte[] data(){
        return this._data;
    }

    private ByteBuffer _buffer;
    public ByteBuffer buffer(){
        return this._buffer;
    }

    public int position() {
        return this._buffer.position();
    }
    public void position(int newPosition){
        this._buffer.position(newPosition);
    }
    public int limit() {
        return this._buffer.limit();
    }
    public void limit(int newLimit) {
        this._buffer.limit(newLimit);
    }
    public int capacity() {
        return this._buffer.capacity();
    }
    public int remaining() {
        return this._buffer.remaining();
    }
    public boolean hasRemaining() {
        return this._buffer.hasRemaining();
    }

    public GrowableByteBuffer(){
        this(1000);
    }

    public GrowableByteBuffer(int initialBytesCapacity){
        assert initialBytesCapacity > 0;
        this._data = new byte[initialBytesCapacity];
        this._buffer = ByteBuffer.wrap(this._data);
    }

    public static GrowableByteBuffer wrap(byte[] data){
        GrowableByteBuffer buffer = new GrowableByteBuffer(false);
        buffer._data = data;
        buffer._buffer = ByteBuffer.wrap(data);
        return buffer;
    }

    private GrowableByteBuffer(boolean flag){
    }

    public ByteBuffer ensureBufferRemaning(int requiredExtraBytes){
        assert requiredExtraBytes > 0;

        if(requiredExtraBytes > remaining()){
            growBuffer(requiredExtraBytes - remaining());
        }

        return this._buffer;
    }

    private static final int _maxGrowBytes = 1000 * 512;

    public ByteBuffer growBuffer(int minGrowBytes){
        assert minGrowBytes > 0;

        byte[] old = this._data;
        int growBytes = capacity() > minGrowBytes ? capacity() : minGrowBytes;
        if(growBytes > _maxGrowBytes && minGrowBytes < _maxGrowBytes) {
            growBytes = _maxGrowBytes;
        }

        this._data = Arrays.copyOf(old, old.length + growBytes);
        this._buffer = ByteBuffer.wrap(this._data, position(), this._data.length - position());
        return this._buffer;
    }

}
