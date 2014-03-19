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
package com.datatorrent.lib.statistics;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Functional Test for {@link WeightedMeanOperator}. <br>
 */
public class StandardDeviationKeyValTest {
    @Test
    public void testStandardDeviation() {
        StandardDeviationKeyVal<String, Long> oper = new StandardDeviationKeyVal<String, Long>();
        CollectorTestSink<Object> variance = new CollectorTestSink<Object>();
        oper.variance.setSink(variance);
        CollectorTestSink<Object> deviation = new CollectorTestSink<Object>();
        oper.standardDeviation.setSink(deviation);

        oper.setup(null);
        oper.beginWindow(0);
        oper.data.process(new KeyValPair<String, Long>("a", new Long(1)));
        oper.data.process(new KeyValPair<String, Long>("a", new Long(7)));
        oper.data.process(new KeyValPair<String, Long>("a", new Long(3)));
        oper.data.process(new KeyValPair<String, Long>("a", new Long(9)));

        oper.data.process(new KeyValPair<String, Long>("b", new Long(20)));
        oper.data.process(new KeyValPair<String, Long>("b", new Long(12)));
        oper.data.process(new KeyValPair<String, Long>("b", new Long(30)));
        oper.endWindow();

        Assert.assertEquals("Must be one tuple in sink", 2, variance.collectedTuples.size());
        Assert.assertEquals("Must be one tuple in sink", 2, deviation.collectedTuples.size());
        System.out.println(variance.collectedTuples.get(0));
        System.out.println(deviation.collectedTuples.get(0));
        System.out.println(variance.collectedTuples.get(1));
        System.out.println(deviation.collectedTuples.get(1));
    }
}
