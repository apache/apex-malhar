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

import org.apache.apex.malhar.lib.streamquery.condition.Condition;
import org.apache.apex.malhar.lib.streamquery.condition.JoinColumnEqualCondition;
import org.apache.apex.malhar.lib.streamquery.index.ColumnIndex;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

@Deprecated
public class FullOuterJoinOperatorTest
{
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSqlSelect()
  {
    // create operator
    OuterJoinOperator oper = new OuterJoinOperator();
    oper.setFullJoin(true);
    CollectorTestSink sink = new CollectorTestSink();
    oper.outport.setSink(sink);

    // set column join condition
    Condition cond = new JoinColumnEqualCondition("a", "a");
    oper.setJoinCondition(cond);

    // add columns
    oper.selectTable1Column(new ColumnIndex("b", null));
    oper.selectTable2Column(new ColumnIndex("c", null));

    oper.setup(null);
    oper.beginWindow(1);

    HashMap<String, Object> tuple = new HashMap<String, Object>();
    tuple.put("a", 0);
    tuple.put("b", 1);
    tuple.put("c", 2);
    oper.inport1.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 3);
    tuple.put("c", 4);
    oper.inport1.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 2);
    tuple.put("b", 11);
    tuple.put("c", 12);
    oper.inport1.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 0);
    tuple.put("b", 7);
    tuple.put("c", 8);
    oper.inport2.process(tuple);

    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 5);
    tuple.put("c", 6);
    oper.inport2.process(tuple);

    oper.endWindow();
    oper.teardown();

    LOG.debug("{}", sink.collectedTuples);
  }

  private static final Logger LOG = LoggerFactory.getLogger(FullOuterJoinOperatorTest.class);

}
