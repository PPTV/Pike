package com.pplive.pike.exec.spoutproto;


import java.util.Arrays;


// tuple serialized format:
//   -------------------------------------------------------
//   |TYPE(1byte)|INDEX(1+bytes)|    VALUE(1+bytes)        |
//   -------------------------------------------------------
// TYPE is assigned byte id of ColumnType
// INDEX is column index of the field in the streaming table
// VALUE is data, see format comments in SimpleBinarySerializer

public class SpoutTupleSerializer {

    public static class TupleField {
        private final int _columnIndex;
        public int columnIndex() { return _columnIndex; }

        private final ColumnType _columnType;
        public ColumnType columnType() { return _columnType; }

        private final Object _value;
        public Object value() { return _value; }

        public TupleField(int columnIndex, ColumnType type, Object val){
            this._columnIndex = columnIndex;
            this._columnType = type;
            this._value = val;
        }

        @Override
        public String toString(){
            return String.format("{%d, %s, %s}", _columnIndex, _columnType, _value);
        }
    }

    private SimpleBinarySerializer _serializer;

    public void beginSerializeTuple(){
        beginSerializeTuple(1000);
    }

    //  serialized format version 1:
//   ------------------------------------------------------------
//   |MAGIC (1byte)|VERSION (1byte)| DATA (all remaining bytes) |
//   ------------------------------------------------------------
//  serialized format version 2+:
//   -------------------------------------------------------------------------------
//   |MAGIC (1byte)|VERSION (1byte)| FLAGS (2bytes) |   DATA (all remaining bytes) |
//   -------------------------------------------------------------------------------

    private static final byte _MagicFlag = (byte)0xec;
    private static final byte _Version_1 = (byte)1;
    private static final byte _Version_2 = (byte)2;

    private static final byte _Flag_TupleList = 0x01;

    public void beginSerializeTuple(int initialBufferBytes){
        if (initialBufferBytes <= 0){
            throw new IllegalArgumentException("initialBufferBytes must be > 0");
        }
        this._serializer = new SimpleBinarySerializer();
        GrowableByteBuffer buffer = new GrowableByteBuffer(initialBufferBytes);
        this._serializer.beginSerialize(buffer);
        this._serializer.addByte(_MagicFlag);
        this._serializer.addByte(_Version_1);
    }

    public byte[] endSerializeTuple(){
        GrowableByteBuffer buffer = this._serializer.buffer();
        byte[] result = Arrays.copyOf(buffer.data(), buffer.position());
        this._serializer = null;
        return result;
    }

    public void addTupleField(int columnIndex, ColumnType type, String textVal) {
        Object obj = textVal;
        boolean serializable = serializable(type);
        if(serializable){
            obj = type.tryParse(textVal);
        }
        addParsedTupleField(columnIndex, type, obj);
    }

    public void addParsedTupleField(int columnIndex, ColumnType type, Object val) {
        if (columnIndex < 0){
            throw new IllegalArgumentException("columnIndex must be > 0");
        }
        if (type == null){
            throw new IllegalArgumentException("type cannot be null");
        }

        byte typeId = ColumnTypeToIntId(type);
        Object obj = val;
        boolean serializable = serializable(type);
        if (obj == null){
            typeId |= 0x80;
        }
        this._serializer.addByte(typeId);
        this._serializer.addInt32(columnIndex - 63); // optimize, since columnIndex always >= 0,
                                                     // so this make larger index (up to 127) can be put in only one byte
        if (obj == null) {
            return;
        }
        switch(type){
            case Boolean:
                this._serializer.addBoolean((Boolean)obj);
                break;
            case String:
                this._serializer.addString(obj.toString());
                break;
            case Byte:
                this._serializer.addByte((Byte) obj);
                break;
            case Short:
                this._serializer.addInt16((Short) obj);
                break;
            case Int:
                this._serializer.addInt32((Integer) obj);
                break;
            case Long:
                this._serializer.addInt64((Long) obj);
                break;
            case Float:
                this._serializer.addFloat((Float) obj);
                break;
            case Double:
                this._serializer.addDouble((Double) obj);
                break;
            case Date:
                java.sql.Date date = (java.sql.Date)obj;
                this._serializer.addInt64(date.getTime());
                break;
            case Time:
                java.sql.Time time = (java.sql.Time)obj;
                this._serializer.addInt64(time.getTime());
                break;
            case Timestamp:
                java.sql.Timestamp timestamp = (java.sql.Timestamp)obj;
                this._serializer.addInt64(timestamp.getTime());
                break;
            default:
                this._serializer.addString(obj.toString());
                break;
        }
    }

    public static boolean isSpoutProtoData_Version1(byte[] data) {
        if (data == null){
            throw new IllegalArgumentException("data cannot be null");
        }
        return data.length >= 2
                && data[0] == _MagicFlag
                && data[1] == _Version_1;
    }

    public static boolean isSpoutProtoData_Version2(byte[] data) {
        if (data == null){
            throw new IllegalArgumentException("data cannot be null");
        }
        return data.length >= 2
                && data[0] == _MagicFlag
                && data[1] == _Version_2;
    }

    public void beginDeserializeTuple(byte[] data){
        if (data == null){
            throw new IllegalArgumentException("data cannot be null");
        }

        this._serializer = new SimpleBinarySerializer();
        GrowableByteBuffer buffer = GrowableByteBuffer.wrap(data);
        this._serializer.beginDeserialize(buffer);

        byte magic = this._serializer.nextByte();
        byte version = this._serializer.nextByte();
        if (magic != _MagicFlag){
            throw new SpoutTupleDeserializeException("error: tuple magic flag unmatch");
        }
        if (version != _Version_1 && version != _Version_2){
            throw new SpoutTupleDeserializeException(String.format("error: tuple version unexpected: %d", version));
        }
    }

    public boolean hasMoreToDeserialize(){
        return this._serializer.buffer().hasRemaining();
    }

    public void endDeserialize(){
        this._serializer = null;
    }

    public TupleField nextField(){
        if (hasMoreToDeserialize() == false){
            return null;
        }

        byte typeId = this._serializer.nextByte();
        int columnIndex = this._serializer.nextInt32() + 63; // see comments in addTupleField()
        boolean nullObj = (typeId & 0x80) != 0;
        typeId &= 0x7f;
        Object val = null;
        ColumnType type = IntIdToColumnType(typeId);
        if (nullObj == false){
            switch(type){
                case Unknown:
                    val = this._serializer.nextString();
                    break;
                case Boolean:
                    val = this._serializer.nextBoolean();
                    break;
                case String:
                    val = this._serializer.nextString();
                    break;
                case Byte:
                    val = this._serializer.nextByte();
                    break;
                case Short:
                    val = this._serializer.nextInt16();
                    break;
                case Int:
                    val = this._serializer.nextInt32();
                    break;
                case Long:
                    val = this._serializer.nextInt64();
                    break;
                case Float:
                    val = this._serializer.nextFloat();
                    break;
                case Double:
                    val = this._serializer.nextDouble();
                    break;
                case Map_ObjString:
                    val = this._serializer.nextString();
                    break;
                case Date:
                    long milliseconds = this._serializer.nextInt64();
                    val = new java.sql.Date(milliseconds);
                    break;
                case Time:
                    milliseconds = this._serializer.nextInt64();
                    val = new java.sql.Time(milliseconds);
                    break;
                case Timestamp:
                    milliseconds = this._serializer.nextInt64();
                    val = new java.sql.Timestamp(milliseconds);
                    break;
                default:
                    assert false;
            }
        }

        return new TupleField(columnIndex, type, val);
    }

    private static boolean serializable(ColumnType type){
        return type != ColumnType.Unknown && type != ColumnType.Map_ObjString;
    }

    private static byte ColumnTypeToIntId(ColumnType type){
        assert type != null;

        switch(type){
            case Unknown: return (byte)0;
            case Boolean: return (byte)1;
            case String: return (byte)2;
            case Byte: return (byte)3;
            case Short: return (byte)4;
            case Int: return (byte)5;
            case Long: return (byte)6;
            case Float: return (byte)7;
            case Double: return (byte)8;
            case Map_ObjString: return (byte)9;
            case Date: return (byte)10;
            case Time: return (byte)11;
            case Timestamp: return (byte)12;
        }
        return 0;
    }

    private static ColumnType IntIdToColumnType(byte typeId){
        switch(typeId){
            case 0: return ColumnType.Unknown;
            case 1: return ColumnType.Boolean;
            case 2: return ColumnType.String;
            case 3: return ColumnType.Byte;
            case 4: return ColumnType.Short;
            case 5: return ColumnType.Int;
            case 6: return ColumnType.Long;
            case 7: return ColumnType.Float;
            case 8: return ColumnType.Double;
            case 9: return ColumnType.Map_ObjString;
            case 10: return ColumnType.Date;
            case 11: return ColumnType.Time;
            case 12: return ColumnType.Timestamp;
        }
        return ColumnType.Unknown;
    }


    public void beginSerializeTuples() {
        beginSerializeTuples(1000 * 128);
    }

    public void beginSerializeTuples(int initialBufferBytes) {
        if (initialBufferBytes <= 0){
            throw new IllegalArgumentException("initialBufferBytes must be > 0");
        }
        this._serializer = new SimpleBinarySerializer();
        GrowableByteBuffer buffer = new GrowableByteBuffer(initialBufferBytes);
        this._serializer.beginSerialize(buffer);
        this._serializer.addByte(_MagicFlag);
        this._serializer.addByte(_Version_2);
        this._serializer.addByte(_Flag_TupleList);
        this._serializer.addByte((byte)0);
    }

    public byte[] endSerializeTuples(){
        GrowableByteBuffer buffer = this._serializer.buffer();
        byte[] result = Arrays.copyOf(buffer.data(), buffer.position());
        this._serializer = null;
        return result;
    }

    public void addSerializedTuple(byte[] data) {
        this._serializer.addBytes(data);
    }

    public void beginDeserializeTuples(byte[] data){
        if (data == null || data.length < 4){
            throw new IllegalArgumentException("data cannot be null and length must >= 4");
        }

        this._serializer = new SimpleBinarySerializer();
        GrowableByteBuffer buffer = GrowableByteBuffer.wrap(data);
        this._serializer.beginDeserialize(buffer);

        byte magic = this._serializer.nextByte();
        byte version = this._serializer.nextByte();
        if (magic != _MagicFlag){
            throw new SpoutTupleDeserializeException("error: tuple magic flag unmatch");
        }
        if (version != _Version_2){
            throw new SpoutTupleDeserializeException(String.format("error: tuple list version unexpected: %d", version));
        }

        byte flag = this._serializer.nextByte();  // first flag byte
        assert (flag & _Flag_TupleList) != 0;
        this._serializer.nextByte(); // skip second flag byte
    }

    public void endDeserializeTuples(){
        this._serializer = null;
    }

    public byte[] nextSerializedTuple() {
        return this._serializer.nextBytes();
    }
}
