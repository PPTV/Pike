package com.pplive.pike.exec.spoutproto;


import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.exec.spoutproto.SpoutTupleSerializer;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class SpoutTupleSerializerTest extends BaseUnitTest {

    @Test
    public void testAll(){
        SpoutTupleSerializer serializer = new SpoutTupleSerializer();
        serializer.beginSerializeTuple();

        Random rand = new Random();
        int randInt = rand.nextInt();
        long randLong = rand.nextLong();
        float randFloat = rand.nextFloat();
        double randDouble = rand.nextDouble();

        serializer.addParsedTupleField(0, ColumnType.Boolean, Boolean.TRUE);
        serializer.addParsedTupleField(1, ColumnType.Boolean, Boolean.FALSE);
        serializer.addParsedTupleField(2, ColumnType.Boolean, null);

        serializer.addParsedTupleField(3, ColumnType.Byte, Byte.valueOf((byte)randInt));
        serializer.addParsedTupleField(4, ColumnType.Byte, null);
        serializer.addParsedTupleField(5, ColumnType.Short, Short.valueOf((short)randInt));
        serializer.addParsedTupleField(6, ColumnType.Short, null);
        serializer.addParsedTupleField(7, ColumnType.Int, Integer.valueOf(randInt));
        serializer.addParsedTupleField(8, ColumnType.Int, null);
        serializer.addParsedTupleField(9, ColumnType.Long, Long.valueOf(randLong));
        serializer.addParsedTupleField(10, ColumnType.Long, null);

        serializer.addParsedTupleField(11, ColumnType.Float, Float.valueOf(randFloat));
        serializer.addParsedTupleField(12, ColumnType.Float, null);
        serializer.addParsedTupleField(13, ColumnType.Double, Double.valueOf(randDouble));
        serializer.addParsedTupleField(14, ColumnType.Double, null);

        serializer.addTupleField(15, ColumnType.Map_ObjString, "{}");
        serializer.addParsedTupleField(16, ColumnType.Map_ObjString, null);

        long milliseconds = new java.util.Date().getTime();
        serializer.addParsedTupleField(17, ColumnType.Date, new java.sql.Date(milliseconds));
        serializer.addParsedTupleField(18, ColumnType.Date, null);
        serializer.addParsedTupleField(19, ColumnType.Time, new java.sql.Time(milliseconds));
        serializer.addParsedTupleField(20, ColumnType.Time, null);
        serializer.addParsedTupleField(21, ColumnType.Timestamp, new java.sql.Timestamp(milliseconds));
        serializer.addParsedTupleField(22, ColumnType.Timestamp, null);

        serializer.addParsedTupleField(23, ColumnType.String, null);
        serializer.addParsedTupleField(24, ColumnType.String, "");
        serializer.addParsedTupleField(25, ColumnType.String, " ");
        serializer.addParsedTupleField(26, ColumnType.String, "\r\n\t");
        serializer.addParsedTupleField(27, ColumnType.String, "\r\n\t中文");

        byte[] data = serializer.endSerializeTuple();
        System.out.println("tuple data bytes: " + data.length);

        serializer.beginDeserializeTuple(data);

        SpoutTupleSerializer.TupleField f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 0);
        Assert.assertTrue(f.columnType() == ColumnType.Boolean);
        Assert.assertTrue(((Boolean)f.value()) == true);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 1);
        Assert.assertTrue(f.columnType() == ColumnType.Boolean);
        Assert.assertTrue(((Boolean)f.value()) == false);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 2);
        Assert.assertTrue(f.columnType() == ColumnType.Boolean);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 3);
        Assert.assertTrue(f.columnType() == ColumnType.Byte);
        Assert.assertTrue(((Byte)f.value()).byteValue() == (byte)randInt);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 4);
        Assert.assertTrue(f.columnType() == ColumnType.Byte);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 5);
        Assert.assertTrue(f.columnType() == ColumnType.Short);
        Assert.assertTrue(((Short)f.value()).shortValue() == (short)randInt);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 6);
        Assert.assertTrue(f.columnType() == ColumnType.Short);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 7);
        Assert.assertTrue(f.columnType() == ColumnType.Int);
        Assert.assertTrue(((Integer)f.value()).intValue() == randInt);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 8);
        Assert.assertTrue(f.columnType() == ColumnType.Int);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 9);
        Assert.assertTrue(f.columnType() == ColumnType.Long);
        Assert.assertTrue(((Long)f.value()).longValue() == randLong);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 10);
        Assert.assertTrue(f.columnType() == ColumnType.Long);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 11);
        Assert.assertTrue(f.columnType() == ColumnType.Float);
        Assert.assertTrue(((Float)f.value()).floatValue() == randFloat);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 12);
        Assert.assertTrue(f.columnType() == ColumnType.Float);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 13);
        Assert.assertTrue(f.columnType() == ColumnType.Double);
        Assert.assertTrue(((Double)f.value()).doubleValue() == randDouble);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 14);
        Assert.assertTrue(f.columnType() == ColumnType.Double);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 15);
        Assert.assertTrue(f.columnType() == ColumnType.Map_ObjString);
        Assert.assertTrue(f.value().toString().equals("{}"));

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 16);
        Assert.assertTrue(f.columnType() == ColumnType.Map_ObjString);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 17);
        Assert.assertTrue(f.columnType() == ColumnType.Date);
        Assert.assertTrue(((java.sql.Date)f.value()).getTime() == milliseconds);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 18);
        Assert.assertTrue(f.columnType() == ColumnType.Date);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 19);
        Assert.assertTrue(f.columnType() == ColumnType.Time);
        Assert.assertTrue(((java.sql.Time)f.value()).getTime() == milliseconds);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 20);
        Assert.assertTrue(f.columnType() == ColumnType.Time);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 21);
        Assert.assertTrue(f.columnType() == ColumnType.Timestamp);
        Assert.assertTrue(((java.sql.Timestamp)f.value()).getTime() == milliseconds);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 22);
        Assert.assertTrue(f.columnType() == ColumnType.Timestamp);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 23);
        Assert.assertTrue(f.columnType() == ColumnType.String);
        Assert.assertTrue(f.value() == null);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() ==24);
        Assert.assertTrue(f.columnType() == ColumnType.String);
        Assert.assertTrue(f.value().toString().equals(""));

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() ==25);
        Assert.assertTrue(f.columnType() == ColumnType.String);
        Assert.assertTrue(f.value().toString().equals(" "));

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() ==26);
        Assert.assertTrue(f.columnType() == ColumnType.String);
        Assert.assertTrue(f.value().toString().equals("\r\n\t"));

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() ==27);
        Assert.assertTrue(f.columnType() == ColumnType.String);
        Assert.assertTrue(f.value().toString().equals("\r\n\t中文"));

        Assert.assertFalse(serializer.hasMoreToDeserialize());
    }

    @Test
    public void testSerializeTupleList() {
        SpoutTupleSerializer serializer = new SpoutTupleSerializer();
        serializer.beginSerializeTuple();

        Random rand = new Random();
        int randInt1 = rand.nextInt();
        long randLong1 = rand.nextLong();
        float randFloat1 = rand.nextFloat();
        double randDouble1 = rand.nextDouble();

        serializer.addParsedTupleField(0, ColumnType.Int, Integer.valueOf(randInt1));
        serializer.addParsedTupleField(1, ColumnType.Long, Long.valueOf(randLong1));
        serializer.addParsedTupleField(2, ColumnType.Float, Float.valueOf(randFloat1));
        serializer.addParsedTupleField(3, ColumnType.Double, Double.valueOf(randDouble1));
        serializer.addParsedTupleField(4, ColumnType.String, "\r\n\t中文");

        byte[] tupleData1 = serializer.endSerializeTuple();

        int randInt2 = rand.nextInt();
        long randLong2 = rand.nextLong();
        float randFloat2 = rand.nextFloat();
        double randDouble2 = rand.nextDouble();

        serializer.beginSerializeTuple();
        serializer.addParsedTupleField(0, ColumnType.Int, Integer.valueOf(randInt2));
        serializer.addParsedTupleField(1, ColumnType.Long, Long.valueOf(randLong2));
        serializer.addParsedTupleField(2, ColumnType.Float, Float.valueOf(randFloat2));
        serializer.addParsedTupleField(3, ColumnType.Double, Double.valueOf(randDouble2));
        serializer.addParsedTupleField(4, ColumnType.String, "\r\n\t中文");

        byte[] tupleData2 = serializer.endSerializeTuple();

        serializer.beginSerializeTuples();
        serializer.addSerializedTuple(tupleData1);
        serializer.addSerializedTuple(tupleData2);
        byte[] data = serializer.endSerializeTuples();

        serializer.beginDeserializeTuples(data);
        byte[] parsedData1 = serializer.nextSerializedTuple();
        byte[] parsedData2 = serializer.nextSerializedTuple();
        Assert.assertFalse(serializer.hasMoreToDeserialize());
        serializer.endDeserializeTuples();

        serializer.beginDeserializeTuple(parsedData1);
        SpoutTupleSerializer.TupleField f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 0);
        Assert.assertTrue(f.columnType() == ColumnType.Int);
        Assert.assertTrue(((Integer)f.value()).intValue() == randInt1);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 1);
        Assert.assertTrue(f.columnType() == ColumnType.Long);
        Assert.assertTrue(((Long)f.value()).longValue() == randLong1);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 2);
        Assert.assertTrue(f.columnType() == ColumnType.Float);
        Assert.assertTrue(((Float)f.value()).floatValue() == randFloat1);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 3);
        Assert.assertTrue(f.columnType() == ColumnType.Double);
        Assert.assertTrue(((Double)f.value()).doubleValue() == randDouble1);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 4);
        Assert.assertTrue(f.columnType() == ColumnType.String);
        Assert.assertTrue(f.value().toString().equals("\r\n\t中文"));

        Assert.assertFalse(serializer.hasMoreToDeserialize());
        serializer.endDeserialize();

        serializer.beginDeserializeTuple(parsedData2);
        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 0);
        Assert.assertTrue(f.columnType() == ColumnType.Int);
        Assert.assertTrue(((Integer)f.value()).intValue() == randInt2);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 1);
        Assert.assertTrue(f.columnType() == ColumnType.Long);
        Assert.assertTrue(((Long)f.value()).longValue() == randLong2);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 2);
        Assert.assertTrue(f.columnType() == ColumnType.Float);
        Assert.assertTrue(((Float)f.value()).floatValue() == randFloat2);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 3);
        Assert.assertTrue(f.columnType() == ColumnType.Double);
        Assert.assertTrue(((Double)f.value()).doubleValue() == randDouble2);

        f = serializer.nextField();
        Assert.assertTrue(f.columnIndex() == 4);
        Assert.assertTrue(f.columnType() == ColumnType.String);
        Assert.assertTrue(f.value().toString().equals("\r\n\t中文"));

        Assert.assertFalse(serializer.hasMoreToDeserialize());
        serializer.endDeserialize();
    }

    private static boolean equalsBytes(byte[] l, byte[] r) {
        if (l.length != r.length)
            return false;
        for(int n = 0; n < l.length; n += 1) {
            if (l[n] != r[n])
                return false;
        }
        return true;
    }
}
