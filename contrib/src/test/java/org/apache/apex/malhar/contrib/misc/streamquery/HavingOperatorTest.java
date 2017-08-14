/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.misc.streamquery;

import java.util.HashMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.misc.streamquery.condition.EqualValueCondition;
import org.apache.apex.malhar.contrib.misc.streamquery.condition.HavingCompareValue;
import org.apache.apex.malhar.contrib.misc.streamquery.condition.HavingCondition;
import org.apache.apex.malhar.contrib.misc.streamquery.function.FunctionIndex;
import org.apache.apex.malhar.contrib.misc.streamquery.function.SumFunction;
import org.apache.apex.malhar.lib.streamquery.index.ColumnIndex;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link org.apache.apex.malhar.contrib.misc.streamquery.HavingOperatorTest}.
 * @deprecated
 */
@Deprecated
public class HavingOperatorTest
{
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSqlGroupBy() throws Exception
  {
    // create operator
    GroupByHavingOperator oper = new GroupByHavingOperator();
    oper.addColumnGroupByIndex(new ColumnIndex("b", null));
    FunctionIndex sum = new SumFunction("c", null);
    oper.addAggregateIndex(sum);

    // create having condition
    HavingCondition having = new HavingCompareValue<Double>(sum, 6.0, 0);
    oper.addHavingCondition(having);

    EqualValueCondition condition = new EqualValueCondition();
    condition.addEqualValue("a", 1);
    oper.setCondition(condition);

    CollectorTestSink sink = new CollectorTestSink();
    oper.outport.setSink(sink);

    oper.setup(null);
    oper.beginWindow(1);

    HashMap<String, Object> tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 1);
    tuple.put("c", 2);
    oper.inport.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 1);
    tuple.put("c", 4);
    oper.inport.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 2);
    tuple.put("c", 6);
    oper.inport.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 2);
    tuple.put("c", 7);
    oper.inport.process(tuple);

    oper.endWindow();
    oper.teardown();

    LOG.debug("{}", sink.collectedTuples);
  }

  private static final Logger LOG = LoggerFactory.getLogger(HavingOperatorTest.class);

}
