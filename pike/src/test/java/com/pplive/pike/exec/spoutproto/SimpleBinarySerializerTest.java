package com.pplive.pike.exec.spoutproto;


import com.pplive.pike.BaseUnitTest;
import com.pplive.pike.exec.spoutproto.GrowableByteBuffer;
import com.pplive.pike.exec.spoutproto.SimpleBinarySerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class SimpleBinarySerializerTest  extends BaseUnitTest {

    @Test
    public void testAll(){
        GrowableByteBuffer buffer = new GrowableByteBuffer(1);
        SimpleBinarySerializer serializer = new SimpleBinarySerializer();
        serializer.beginSerialize(buffer);

        serializer.addBoolean(true);
        serializer.addBoolean(false);

        serializer.addByte((byte)0);
        serializer.addByte((byte)1);
        serializer.addByte((byte)-1);
        serializer.addByte(Byte.MAX_VALUE);
        serializer.addByte(Byte.MIN_VALUE);

        serializer.addInt16((short) 0);
        serializer.addInt16((short) 1);
        serializer.addInt16((short) -1);
        serializer.addInt16(Short.MAX_VALUE);
        serializer.addInt16(Short.MIN_VALUE);

        serializer.addInt32(0);
        serializer.addInt32(1);
        serializer.addInt32(-1);
        serializer.addInt32(Integer.MAX_VALUE);
        serializer.addInt32(Integer.MIN_VALUE);

        serializer.addInt64(0);
        serializer.addInt64(1);
        serializer.addInt64(-1);
        serializer.addInt64(Long.MAX_VALUE);
        serializer.addInt64(Long.MIN_VALUE + 1);
        serializer.addInt64(Long.MIN_VALUE);

        serializer.addFloat(0.0f);
        serializer.addFloat(1.0f);
        serializer.addFloat(-1.0f);
        serializer.addFloat(0.1f);
        serializer.addFloat(-0.1f);
        serializer.addFloat(Float.MAX_VALUE);
        serializer.addFloat(Float.MIN_VALUE);
        serializer.addFloat(Float.MIN_NORMAL);
        serializer.addFloat(Float.POSITIVE_INFINITY);
        serializer.addFloat(Float.NEGATIVE_INFINITY);
        serializer.addFloat(Float.NaN);

        serializer.addDouble(0.0d);
        serializer.addDouble(1.0d);
        serializer.addDouble(-1.0d);
        serializer.addDouble(0.1d);
        serializer.addDouble(-0.1d);
        serializer.addDouble(Double.MAX_VALUE);
        serializer.addDouble(Double.MIN_VALUE);
        serializer.addDouble(Double.MIN_NORMAL);
        serializer.addDouble(Double.POSITIVE_INFINITY);
        serializer.addDouble(Double.NEGATIVE_INFINITY);
        serializer.addDouble(Double.NaN);

        serializer.addString("");
        serializer.addString("a");
        serializer.addString("ab");
        serializer.addString("abc");
        serializer.addString("abc中文");
        serializer.addString(" ");
        serializer.addString("\t\r\n");

        serializer.addBytes(new byte[0]);

        Random rand = new Random();
        int randInt = rand.nextInt();
        long randLong = rand.nextLong();
        float randFloat = rand.nextFloat();
        double randDouble = rand.nextDouble();
        serializer.addInt32(randInt);
        serializer.addInt64(randLong);
        serializer.addFloat(randFloat);
        serializer.addDouble(randDouble);

        int size = rand.nextInt(20);
        byte[] randBytes = new byte[1 + size];
        rand.nextBytes(randBytes);
        serializer.addBytes(randBytes);


        buffer = serializer.endSerialize();
        System.out.println("data bytes: " + buffer.position());
        buffer.buffer().position(0);


        serializer.beginDeserialize(buffer);
        Assert.assertTrue(serializer.nextBoolean() == true);
        Assert.assertTrue(serializer.nextBoolean() == false);

        Assert.assertTrue(serializer.nextByte() == 0);
        Assert.assertTrue(serializer.nextByte() == 1);
        Assert.assertTrue(serializer.nextByte() == -1);
        Assert.assertTrue(serializer.nextByte() == Byte.MAX_VALUE);
        Assert.assertTrue(serializer.nextByte() == Byte.MIN_VALUE);

        Assert.assertTrue(serializer.nextInt16() == 0);
        Assert.assertTrue(serializer.nextInt16() == 1);
        Assert.assertTrue(serializer.nextInt16() == -1);
        Assert.assertTrue(serializer.nextInt16() == Short.MAX_VALUE);
        Assert.assertTrue(serializer.nextInt16() == Short.MIN_VALUE);

        Assert.assertTrue(serializer.nextInt32() == 0);
        Assert.assertTrue(serializer.nextInt32() == 1);
        Assert.assertTrue(serializer.nextInt32() == -1);
        Assert.assertTrue(serializer.nextInt32() == Integer.MAX_VALUE);
        Assert.assertTrue(serializer.nextInt32() == Integer.MIN_VALUE);

        Assert.assertTrue(serializer.nextInt64() == 0);
        Assert.assertTrue(serializer.nextInt64() == 1);
        Assert.assertTrue(serializer.nextInt64() == -1);
        Assert.assertTrue(serializer.nextInt64() == Long.MAX_VALUE);
        Assert.assertTrue(serializer.nextInt64() == Long.MIN_VALUE + 1);
        Assert.assertTrue(serializer.nextInt64() == Long.MIN_VALUE);

        Assert.assertTrue(serializer.nextFloat() == 0.0f);
        Assert.assertTrue(serializer.nextFloat() == 1.0f);
        Assert.assertTrue(serializer.nextFloat() == -1.0f);
        Assert.assertTrue(serializer.nextFloat() == 0.1f);
        Assert.assertTrue(serializer.nextFloat() == -0.1f);
        Assert.assertTrue(serializer.nextFloat() == Float.MAX_VALUE);
        Assert.assertTrue(serializer.nextFloat() == Float.MIN_VALUE);
        Assert.assertTrue(serializer.nextFloat() == Float.MIN_NORMAL);
        Assert.assertTrue(Float.isInfinite(serializer.nextFloat()));
        Assert.assertTrue(Float.isInfinite(serializer.nextFloat()));
        Assert.assertTrue(Float.isNaN(serializer.nextFloat()));

        Assert.assertTrue(serializer.nextDouble() == 0.0d);
        Assert.assertTrue(serializer.nextDouble() == 1.0d);
        Assert.assertTrue(serializer.nextDouble() == -1.0d);
        Assert.assertTrue(serializer.nextDouble() == 0.1d);
        Assert.assertTrue(serializer.nextDouble() == -0.1d);
        Assert.assertTrue(serializer.nextDouble() == Double.MAX_VALUE);
        Assert.assertTrue(serializer.nextDouble() == Double.MIN_VALUE);
        Assert.assertTrue(serializer.nextDouble() == Double.MIN_NORMAL);
        Assert.assertTrue(Double.isInfinite(serializer.nextDouble()));
        Assert.assertTrue(Double.isInfinite(serializer.nextDouble()));
        Assert.assertTrue(Double.isNaN(serializer.nextDouble()));

        Assert.assertEquals(serializer.nextString(), "");
        Assert.assertEquals(serializer.nextString(), "a");
        Assert.assertEquals(serializer.nextString(), "ab");
        Assert.assertEquals(serializer.nextString(), "abc");
        Assert.assertEquals(serializer.nextString(), "abc中文");
        Assert.assertEquals(serializer.nextString(), " ");
        Assert.assertEquals(serializer.nextString(), "\t\r\n");

        Assert.assertTrue(serializer.nextBytes().length == 0);

        Assert.assertTrue(serializer.nextInt32() == randInt);
        Assert.assertTrue(serializer.nextInt64() == randLong);
        Assert.assertTrue(serializer.nextFloat() == randFloat);
        Assert.assertTrue(serializer.nextDouble() == randDouble);

        byte[] bytes = serializer.nextBytes();
        Assert.assertTrue(equalsBytes(randBytes, bytes));
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
