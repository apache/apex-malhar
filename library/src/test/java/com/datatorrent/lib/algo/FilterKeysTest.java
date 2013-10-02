package com.datatorrent.lib.algo;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Test class to test FilterKey functionality,
 */
public class FilterKeysTest {

    public final static class MyKey implements FilterKeys.Filterable {

        public String merchantId;
        public Integer terminalId;
        public Integer zipCode;

        @Override
        public boolean filter(FilterKeys.Filterable filter) {
            MyKey key = (MyKey)filter;
            return (this.terminalId == null && this.merchantId == null && this.zipCode == null)
                    || (this.terminalId == null && this.merchantId == null && this.zipCode != null && this.zipCode.intValue() == key.zipCode.intValue())
                    || (this.terminalId == null && this.merchantId != null && this.merchantId.equalsIgnoreCase(key.merchantId) && this.zipCode.intValue() == key.zipCode.intValue())
                    || (this.terminalId != null && this.terminalId.intValue() == key.terminalId.intValue() && this.merchantId != null && this.merchantId.equalsIgnoreCase(key.merchantId) && this.zipCode.intValue() == key.zipCode.intValue());
        }
    }

    @Test
    public void testFilterKeys1() {

        FilterKeys<MyKey, Integer> oper = new FilterKeys<MyKey, Integer>();
        CollectorTestSink testSink = new CollectorTestSink();
        oper.output.setSink(testSink);

        MyKey filter = new MyKey();
        filter.merchantId = "Wal-Mart";
        filter.terminalId = 5;
        filter.zipCode = 94086;

        oper.filter = filter;

        MyKey key = new MyKey();
        key.merchantId = "Wal-Mart-1";
        key.terminalId = 6;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Wal-Mart";
        key.terminalId = 5;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Macy's";
        key.terminalId = 10;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Apple";
        key.terminalId = 12;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Target";
        key.terminalId = 5;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        Assert.assertEquals("number emitted tuples", 1, testSink.collectedTuples.size());
        System.out.println("Filtered: " + testSink.collectedTuples.size());
    }

    @Test
    public void testFilterKeys2() {

        FilterKeys<MyKey, Integer> oper = new FilterKeys<MyKey, Integer>();
        CollectorTestSink testSink = new CollectorTestSink();
        oper.output.setSink(testSink);

        MyKey filter = new MyKey();
        filter.merchantId = "Wal-Mart";
//        filter.terminalId = 5;
        filter.zipCode = 94086;

        oper.filter = filter;

        MyKey key = new MyKey();
        key.merchantId = "Wal-Mart";
        key.terminalId = 6;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Wal-Mart";
        key.terminalId = 5;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Macy's";
        key.terminalId = 10;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Apple";
        key.terminalId = 12;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Target";
        key.terminalId = 5;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        Assert.assertEquals("number emitted tuples", 2, testSink.collectedTuples.size());
        System.out.println("Filtered: " + testSink.collectedTuples.size());
    }

    @Test
    public void testFilterKeys3() {

        FilterKeys<MyKey, Integer> oper = new FilterKeys<MyKey, Integer>();
        CollectorTestSink testSink = new CollectorTestSink();
        oper.output.setSink(testSink);

        MyKey filter = new MyKey();
//        filter.merchantId = "Wal-Mart";
//        filter.terminalId = 5;
//        filter.zipCode = 94086;

        oper.filter = filter;

        MyKey key = new MyKey();
        key.merchantId = "Wal-Mart-1";
        key.terminalId = 6;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Wal-Mart";
        key.terminalId = 5;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Macy's";
        key.terminalId = 10;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Apple";
        key.terminalId = 12;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Target";
        key.terminalId = 5;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        Assert.assertEquals("number emitted tuples", 5, testSink.collectedTuples.size());
        System.out.println("Filtered: " + testSink.collectedTuples.size());
    }

    @Test
    public void testFilterKeys4() {

        FilterKeys<MyKey, Integer> oper = new FilterKeys<MyKey, Integer>();
        CollectorTestSink testSink = new CollectorTestSink();
        oper.output.setSink(testSink);

        MyKey filter = new MyKey();
        filter.merchantId = "Wal-Mart";
        filter.terminalId = 5;
        filter.zipCode = 94086;

        oper.filter = filter;

        MyKey key = new MyKey();
        key.merchantId = "Wal-Mart-1";
        key.terminalId = 6;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Wal-Mart";
        key.terminalId = 10;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Macy's";
        key.terminalId = 10;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Apple";
        key.terminalId = 12;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        key = new MyKey();
        key.merchantId = "Target";
        key.terminalId = 5;
        key.zipCode = 94086;
        oper.filterPort.process(new KeyValPair<MyKey, Integer>(key, 100));

        Assert.assertEquals("number emitted tuples", 0, testSink.collectedTuples.size());
        System.out.println("Filtered: " + testSink.collectedTuples.size());
    }

}
