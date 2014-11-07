/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import java.util.HashMap;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.util.KeyHashValPair;
import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.UniqueCounter}<p>
 *
 */
public class PartitionableUniqueCounterTest
{
    /**
     * Test node logic emits correct results
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testNodeProcessing() throws Exception
    {
        PartitionableUniqueCount<String> oper = new PartitionableUniqueCount<String>();
        CollectorTestSink sink = new CollectorTestSink<KeyHashValPair<String, Integer>>();
        oper.count.setSink(sink);

        String atuple = "a";
        String btuple = "b";
        String ctuple = "c";
        String dtuple = "d";
        String etuple = "e";

        int numTuples = 10000;
        oper.beginWindow(0);
        for (int i = 0; i < numTuples; i++) {
            oper.data.process(atuple);
            if (i % 2 == 0) {
                oper.data.process(btuple);
            }
            if (i % 3 == 0) {
                oper.data.process(ctuple);
            }
            if (i % 5 == 0) {
                oper.data.process(dtuple);
            }
            if (i % 10 == 0) {
                oper.data.process(etuple);
            }
        }
        oper.endWindow();

        HashMap<String, Integer> tuple = new HashMap<String, Integer>();
        for(Object e1 : sink.collectedTuples) {
            KeyHashValPair<String, Integer> e = (KeyHashValPair<String, Integer>)e1;
            tuple.put(e.getKey(), e.getValue());
        }

        int acount = tuple.get("a").intValue();
        int bcount = tuple.get("b").intValue();
        int ccount = tuple.get("c").intValue();
        int dcount = tuple.get("d").intValue();
        int ecount = tuple.get("e").intValue();
        Assert.assertEquals("number emitted tuples", 5, sink.collectedTuples.size());
        Assert.assertEquals("number emitted tuples", numTuples, acount);
        Assert.assertEquals("number emitted tuples", numTuples/2, bcount);
        Assert.assertEquals("number emitted tuples", numTuples/3 + 1, ccount);
        Assert.assertEquals("number emitted tuples", numTuples/5, dcount);
        Assert.assertEquals("number emitted tuples", numTuples/10, ecount);
    }
}
