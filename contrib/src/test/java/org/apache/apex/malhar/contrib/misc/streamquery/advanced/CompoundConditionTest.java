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
package org.apache.apex.malhar.contrib.misc.streamquery.advanced;

import java.util.HashMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.misc.streamquery.SelectOperator;
import org.apache.apex.malhar.contrib.misc.streamquery.condition.CompoundCondition;
import org.apache.apex.malhar.contrib.misc.streamquery.condition.EqualValueCondition;
import org.apache.apex.malhar.lib.streamquery.index.ColumnIndex;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link org.apache.apex.malhar.contrib.misc.streamquery.advanced.CompoundConditionTest}.
 * @deprecated
 */
@Deprecated
public class CompoundConditionTest
{
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSqlSelect()
  {
    // create operator
    SelectOperator oper = new SelectOperator();
    oper.addIndex(new ColumnIndex("b", null));
    oper.addIndex(new ColumnIndex("c", null));

    EqualValueCondition left = new EqualValueCondition();
    left.addEqualValue("a", 1);
    EqualValueCondition  right = new EqualValueCondition();
    right.addEqualValue("b", 1);

    oper.setCondition(new CompoundCondition(left, right));


    CollectorTestSink sink = new CollectorTestSink();
    oper.outport.setSink(sink);

    oper.setup(null);
    oper.beginWindow(1);

    HashMap<String, Object> tuple = new HashMap<String, Object>();
    tuple.put("a", 0);
    tuple.put("b", 1);
    tuple.put("c", 2);
    oper.inport.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 3);
    tuple.put("c", 4);
    oper.inport.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 5);
    tuple.put("c", 6);
    oper.inport.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 3);
    tuple.put("b", 7);
    tuple.put("c", 8);
    oper.inport.process(tuple);

    oper.endWindow();
    oper.teardown();

    LOG.debug("{}", sink.collectedTuples);
  }

  private static final Logger LOG = LoggerFactory.getLogger(CompoundConditionTest.class);
}
