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
package org.apache.apex.malhar.lib.join;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.streamquery.condition.Condition;
import org.apache.apex.malhar.lib.streamquery.condition.JoinColumnEqualCondition;
import org.apache.apex.malhar.lib.streamquery.index.ColumnIndex;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

/**
 *
 * Functional test for {@link SemiJoinOperator }.
 *
 */
public class SemiJoinOperatorTest
{
  @Test
  public void testSqlSelect()
  {
    // create operator
    SemiJoinOperator oper = new SemiJoinOperator();
    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.outport, sink);

    // set column join condition
    Condition cond = new JoinColumnEqualCondition("a", "a");
    oper.setJoinCondition(cond);

    // add columns (only columns from the left table)
    oper.selectTable1Column(new ColumnIndex("b", null));
    oper.selectTable1Column(new ColumnIndex("c", null));

    oper.setup(null);
    HashMap<String, Object> tuple = new HashMap<>();

    /**
     * test 1, positive result
     *
     * table1                      table2
     * ======                      ======
     *    a     b    c             a    e    f
     *    -------------            -----------
     *    0     1    2             0    7    8
     *    1     3    4             2    5    6
     *    0     6    8
     *
     * select b, c
     * from   table1 semi-join table2
     * where  table1.a = table2.a
     *
     * result:
     *
     *         b    c
     *    -------------
     *         1    2
     *         6    8
     */
    oper.beginWindow(1);
    tuple.put("a", 0);
    tuple.put("b", 1);
    tuple.put("c", 2);
    oper.inport1.process(tuple);

    tuple = new HashMap<>();
    tuple.put("a", 1);
    tuple.put("b", 3);
    tuple.put("c", 4);
    oper.inport1.process(tuple);

    tuple = new HashMap<>();
    tuple.put("a", 0);
    tuple.put("b", 6);
    tuple.put("c", 8);

    oper.inport1.process(tuple);
    tuple = new HashMap<>();
    tuple.put("a", 0);
    tuple.put("e", 7);
    tuple.put("f", 8);
    oper.inport2.process(tuple);

    tuple = new HashMap<>();
    tuple.put("a", 2);
    tuple.put("e", 5);
    tuple.put("f", 6);
    oper.inport2.process(tuple);

    oper.endWindow();

    // expected semi-joined result: {1,2}, {6, 8} (two rows)
    Assert.assertEquals("number of semi-join result", 2, sink.collectedTuples.size());
    sink.clear();

    /**
     * test 2, negative result (empty result)
     *
     * table1                      table2
     * ======                      ======
     *    a     b    c             a    e    f
     *    -------------            -----------
     *    1     1    2             0    7    8
     *    1     3    4             2    5    6
     *    3     6    8
     *
     * select b, c
     * from   table1 semi-join table2
     * where  table1.a = table2.a
     *
     * result:
     *
     *         b    c
     *    -------------
     *
     *    (0 row)
     */
    oper.beginWindow(2);
    tuple.put("a", 1);
    tuple.put("b", 1);
    tuple.put("c", 2);
    oper.inport1.process(tuple);

    tuple = new HashMap<>();
    tuple.put("a", 1);
    tuple.put("b", 3);
    tuple.put("c", 4);
    oper.inport1.process(tuple);

    tuple = new HashMap<>();
    tuple.put("a", 3);
    tuple.put("b", 6);
    tuple.put("c", 8);
    oper.inport1.process(tuple);

    tuple = new HashMap<>();
    tuple.put("a", 0);
    tuple.put("e", 7);
    tuple.put("f", 8);
    oper.inport2.process(tuple);

    tuple = new HashMap<>();
    tuple.put("a", 2);
    tuple.put("e", 5);
    tuple.put("f", 6);
    oper.inport2.process(tuple);

    oper.endWindow();
    oper.teardown();

    // expected semi-joined result: [] (empty)
    Assert.assertEquals("number of semi-join result (empty)", 0, sink.collectedTuples.size());
  }
}
