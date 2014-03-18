package com.datatorrent.lib.codec;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.common.util.Slice;
import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link KryoSerializableStreamCodec}
 *
 */
public class KryoStreamCodecTest {

    public static class TestTuple {
        final Integer field;

        public TestTuple(){
            this.field= new Integer(0);
        }

        public TestTuple(Integer x){
            this.field= Preconditions.checkNotNull(x,"x");
        }

        @Override
        public boolean equals(Object o) {
           return o.getClass()== this.getClass() && ((TestTuple)o).field.equals(this.field);
        }

        @Override
        public int hashCode() {
            return TestTuple.class.hashCode()^ this.field.hashCode();
        }
    }

    public static class TestKryoStreamCodec extends KryoSerializableStreamCodec<TestTuple> {

        public TestKryoStreamCodec(){
          super();
        }

        @Override
        public int getPartition(TestTuple testTuple) {
            return testTuple.field;
        }
    }

    @Test
    public void testSomeMethod() throws IOException
    {
        TestKryoStreamCodec coder = new TestKryoStreamCodec();
        TestKryoStreamCodec decoder = new TestKryoStreamCodec();

        KryoSerializableStreamCodec<Object> objCoder = new KryoSerializableStreamCodec<Object>();
        Slice sliceOfObj = objCoder.toByteArray(new Integer(10));
        Object decodedObj = objCoder.fromByteArray(sliceOfObj);

        Assert.assertEquals("codec", decodedObj, new Integer(10));

        TestTuple tp= new TestTuple(new Integer(5));

        Slice dsp1 = coder.toByteArray(tp);
        Slice dsp2 = coder.toByteArray(tp);
        Assert.assertEquals(dsp1, dsp2);

        Object tcObject1 = decoder.fromByteArray(dsp1);
        assert (tp.equals(tcObject1));

        Object tcObject2 = decoder.fromByteArray(dsp2);
        assert (tp.equals(tcObject2));

        dsp1 = coder.toByteArray(tp);
        dsp2 = coder.toByteArray(tp);
        Assert.assertEquals(dsp1, dsp2);
    }

    @Test
    public void testFinalFieldSerialization() throws Exception{
        TestTuple t1 = new TestTuple(5);
        TestKryoStreamCodec codec= new TestKryoStreamCodec();

        Slice dsp = codec.toByteArray(t1);
        TestTuple t2 = (TestTuple)codec.fromByteArray(dsp);
        Assert.assertEquals("", t1.field, t2.field);
    }
}
